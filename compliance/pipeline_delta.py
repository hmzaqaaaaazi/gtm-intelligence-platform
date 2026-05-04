"""
Pipeline Delta Reporter
- Reads compliance/pipeline_snapshot.json  (previous state)
- Queries HubSpot for current deals by stage
- Compares: deals added, removed, changed stage, ACV shifts
- Calls Groq to synthesize a narrative summary
- Posts DRAFT to Slack for human review; leadership post is a separate step
  (triggered via --publish, or a manual GitHub Actions environment approval)
- Saves new snapshot to compliance/pipeline_snapshot.json

Usage:
  python -m compliance.pipeline_delta           # draft mode (default)
  python -m compliance.pipeline_delta --publish # post approved draft to leadership
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HUBSPOT_BASE_URL  = "https://api.hubapi.com"
HUBSPOT_PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "244783142")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
GROQ_API_URL      = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL        = "llama-3.1-8b-instant"

SNAPSHOT_PATH     = Path(__file__).parent / "pipeline_snapshot.json"
DRAFT_PATH        = Path(__file__).parent / "pipeline_delta_draft.json"

STAGE_LABELS = {
    "appointmentscheduled":   "Appointment Scheduled",
    "qualifiedtobuy":         "Qualified to Buy",
    "presentationscheduled":  "Presentation Scheduled",
    "decisionmakerboughtin":  "Decision Maker Bought-In",
    "contractsent":           "Contract Sent",
    "closedwon":              "Closed Won",
    "closedlost":             "Closed Lost",
}
OPEN_STAGES   = {"appointmentscheduled", "qualifiedtobuy", "presentationscheduled",
                 "decisionmakerboughtin", "contractsent"}


# ── HubSpot helpers ────────────────────────────────────────────────────────────

def _hs_headers() -> dict:
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        raise EnvironmentError("HUBSPOT_ACCESS_TOKEN not set.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _deal_link(deal_id: str) -> str:
    return f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/deal/{deal_id}"


# ── Step 1 — Load previous snapshot ──────────────────────────────────────────

def load_snapshot() -> dict:
    if SNAPSHOT_PATH.exists():
        try:
            return json.loads(SNAPSHOT_PATH.read_text())
        except Exception as exc:
            logger.warning("Could not read snapshot: %s — treating as first run.", exc)
    return {"snapshot_date": None, "stages": {}, "deals": {}, "total_deals": 0, "total_value": 0.0}


# ── Step 2 — Fetch current deals from HubSpot ─────────────────────────────────

def fetch_current_deals() -> list[dict]:
    """Fetch ALL deals (open and closed) for full pipeline visibility."""
    url   = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/search"
    after = None
    all_deals: list[dict] = []

    while True:
        payload: dict = {
            "filterGroups": [],   # no filter — all deals
            "properties": [
                "dealname", "amount", "dealstage",
                "hs_lastmodifieddate", "createdate",
                "hubspot_owner_id",
            ],
            "limit": 100,
        }
        if after:
            payload["after"] = after

        try:
            r = requests.post(url, json=payload, headers=_hs_headers(), timeout=30)
            r.raise_for_status()
            body = r.json()
        except Exception as exc:
            logger.error("Failed to fetch deals: %s", exc)
            break

        all_deals.extend(body.get("results", []))
        paging = body.get("paging", {}).get("next", {})
        after  = paging.get("after")
        if not after:
            break

    logger.info("Fetched %d deals from HubSpot.", len(all_deals))
    return all_deals


def build_snapshot(deals: list[dict]) -> dict:
    """Collapse deals into a snapshot dict suitable for diffing."""
    stages: dict[str, dict] = {}
    deals_index: dict[str, dict] = {}      # deal_id → {name, stage, acv}

    for deal in deals:
        props = deal.get("properties", {})
        stage = props.get("dealstage", "unknown")
        try:
            acv = float(props.get("amount") or 0)
        except (ValueError, TypeError):
            acv = 0.0

        stages.setdefault(stage, {"count": 0, "total_value": 0.0, "deal_ids": []})
        stages[stage]["count"]       += 1
        stages[stage]["total_value"] += acv
        stages[stage]["deal_ids"].append(deal["id"])

        deals_index[deal["id"]] = {
            "name":  props.get("dealname") or "Unnamed",
            "stage": stage,
            "acv":   acv,
        }

    total_value = sum(d["acv"] for d in deals_index.values())
    return {
        "snapshot_date": datetime.now(timezone.utc).isoformat(),
        "stages":        stages,
        "deals":         deals_index,
        "total_deals":   len(deals_index),
        "total_value":   round(total_value, 2),
    }


# ── Step 3 — Compute delta ────────────────────────────────────────────────────

def compute_delta(prev: dict, curr: dict) -> dict:
    """
    Returns:
      added          — deal IDs new since last snapshot
      removed        — deal IDs gone since last snapshot
      stage_changes  — deals that moved between stages
      stage_summary  — per-stage count/value changes
      acv_delta      — total pipeline ACV change
    """
    prev_deals: dict = prev.get("deals", {})
    curr_deals: dict = curr.get("deals", {})

    prev_ids = set(prev_deals.keys())
    curr_ids = set(curr_deals.keys())

    added_ids   = curr_ids - prev_ids
    removed_ids = prev_ids - curr_ids

    stage_changes: list[dict] = []
    for did in prev_ids & curr_ids:
        p_stage = prev_deals[did]["stage"]
        c_stage = curr_deals[did]["stage"]
        if p_stage != c_stage:
            stage_changes.append({
                "deal_id":   did,
                "name":      curr_deals[did]["name"],
                "from_stage": STAGE_LABELS.get(p_stage, p_stage),
                "to_stage":   STAGE_LABELS.get(c_stage, c_stage),
                "acv":        curr_deals[did]["acv"],
                "link":       _deal_link(did),
            })

    # Stage-level summary (count/value changes)
    all_stages = set(list(prev.get("stages", {}).keys()) +
                     list(curr.get("stages", {}).keys()))
    stage_summary: list[dict] = []
    for stage in sorted(all_stages):
        p = prev.get("stages", {}).get(stage, {"count": 0, "total_value": 0.0})
        c = curr.get("stages", {}).get(stage, {"count": 0, "total_value": 0.0})
        stage_summary.append({
            "stage":        STAGE_LABELS.get(stage, stage),
            "stage_key":    stage,
            "prev_count":   p["count"],
            "curr_count":   c["count"],
            "count_delta":  c["count"] - p["count"],
            "prev_value":   round(p["total_value"], 2),
            "curr_value":   round(c["total_value"], 2),
            "value_delta":  round(c["total_value"] - p["total_value"], 2),
        })

    return {
        "as_of":               curr["snapshot_date"],
        "prev_snapshot_date":  prev.get("snapshot_date"),
        "added_deals":         [
            {"deal_id": did, **curr_deals[did], "link": _deal_link(did)}
            for did in sorted(added_ids)
        ],
        "removed_deals":       [
            {"deal_id": did, **prev_deals[did]}
            for did in sorted(removed_ids)
        ],
        "stage_changes":       stage_changes,
        "stage_summary":       stage_summary,
        "total_deals_prev":    prev.get("total_deals", 0),
        "total_deals_curr":    curr["total_deals"],
        "total_value_prev":    prev.get("total_value", 0.0),
        "total_value_curr":    curr["total_value"],
        "acv_delta":           round(curr["total_value"] - prev.get("total_value", 0.0), 2),
    }


# ── Step 4 — Groq narrative ───────────────────────────────────────────────────

def generate_narrative(delta: dict) -> str:
    """Ask Groq to write a concise, executive-level pipeline narrative."""
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        logger.warning("GROQ_API_KEY not set — using template narrative.")
        return _template_narrative(delta)

    prompt = f"""You are a revenue operations analyst. Given this pipeline delta summary, 
write a concise 3-4 sentence executive narrative (no bullet points, plain prose) 
suitable for a weekly leadership update. Highlight: total pipeline change, 
notable stage movements, deals added or lost, and any red flags.

Delta data:
{json.dumps(delta, indent=2, default=str)}

Write only the narrative. No headings, no JSON, no preamble."""

    try:
        r = requests.post(
            GROQ_API_URL,
            json={
                "model":       GROQ_MODEL,
                "messages":    [{"role": "user", "content": prompt}],
                "temperature": 0.4,
                "max_tokens":  300,
            },
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type":  "application/json",
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        logger.error("Groq narrative failed: %s", exc)
        return _template_narrative(delta)


def _template_narrative(delta: dict) -> str:
    acv_sign  = "+" if delta["acv_delta"] >= 0 else ""
    prev_date = delta.get("prev_snapshot_date", "baseline")[:10] if delta.get("prev_snapshot_date") else "baseline"
    return (
        f"Pipeline update as of {delta['as_of'][:10]} vs {prev_date}. "
        f"Total open deals: {delta['total_deals_curr']} "
        f"({'+' if delta['total_deals_curr'] - delta['total_deals_prev'] >= 0 else ''}"
        f"{delta['total_deals_curr'] - delta['total_deals_prev']} vs prior). "
        f"Total pipeline value: ${delta['total_value_curr']:,.0f} "
        f"({acv_sign}${delta['acv_delta']:,.0f}). "
        f"{len(delta['added_deals'])} new deal(s) entered, "
        f"{len(delta['removed_deals'])} deal(s) closed or removed, "
        f"{len(delta['stage_changes'])} deal(s) advanced stage."
    )


# ── Step 5 — Build Slack blocks ───────────────────────────────────────────────

def build_slack_message(delta: dict, narrative: str, is_draft: bool) -> dict:
    draft_tag = ":pencil: *[DRAFT — Pending Leadership Approval]*\n" if is_draft else ""
    date_str  = delta["as_of"][:10]
    acv_sign  = "+" if delta["acv_delta"] >= 0 else ""

    # Stage table
    stage_lines = []
    for s in delta["stage_summary"]:
        if s["curr_count"] == 0 and s["prev_count"] == 0:
            continue
        arrow = ("↑" if s["count_delta"] > 0 else "↓" if s["count_delta"] < 0 else "→")
        stage_lines.append(
            f"  {arrow} *{s['stage']}*: {s['curr_count']} deals "
            f"(${s['curr_value']:,.0f}) "
            f"[{'+' if s['count_delta'] >= 0 else ''}{s['count_delta']} deals, "
            f"{'+' if s['value_delta'] >= 0 else ''}${s['value_delta']:,.0f}]"
        )

    # Notable movements
    movements = []
    for sc in delta["stage_changes"][:5]:
        movements.append(
            f"  • <{sc['link']}|{sc['name']}> "
            f"{sc['from_stage']} → *{sc['to_stage']}* "
            f"(${sc['acv']:,.0f})"
        )
    for ad in delta["added_deals"][:3]:
        movements.append(
            f"  • :new: <{ad['link']}|{ad['name']}> entered pipeline "
            f"@ {STAGE_LABELS.get(ad.get('stage',''), ad.get('stage','?'))} "
            f"(${ad['acv']:,.0f})"
        )

    text_parts = [
        f"{draft_tag}:bar_chart: *Weekly Pipeline Delta — {date_str}*\n",
        f"*Summary:* {narrative}\n",
        f"*Pipeline totals:*  "
        f"{delta['total_deals_curr']} deals | "
        f"${delta['total_value_curr']:,.0f} total ACV "
        f"({acv_sign}${delta['acv_delta']:,.0f} WoW)\n",
    ]
    if stage_lines:
        text_parts.append("*By stage:*\n" + "\n".join(stage_lines))
    if movements:
        text_parts.append("\n*Notable movements:*\n" + "\n".join(movements))
    if is_draft:
        text_parts.append(
            "\n_To approve and post to leadership: "
            "trigger the `pipeline_delta` workflow and select *Publish* environment._"
        )

    return {"text": "\n".join(text_parts)}


# ── Step 6 — Post to Slack ────────────────────────────────────────────────────

def post_to_slack(message: dict, label: str = "draft") -> bool:
    webhook = SLACK_WEBHOOK_URL
    if not webhook:
        logger.warning("SLACK_WEBHOOK_URL not set — printing to stdout instead.")
        print("\n── Slack message (not sent) ──")
        print(message["text"])
        return False
    try:
        r = requests.post(webhook, json=message, timeout=15)
        r.raise_for_status()
        logger.info("Pipeline delta %s posted to Slack.", label)
        return True
    except Exception as exc:
        logger.error("Failed to post to Slack (%s): %s", label, exc)
        return False


# ── Step 7 — Save snapshot & commit ──────────────────────────────────────────

def save_snapshot(snapshot: dict) -> None:
    SNAPSHOT_PATH.write_text(json.dumps(snapshot, indent=2, default=str))
    logger.info("Snapshot saved to %s", SNAPSHOT_PATH)


def git_commit_snapshot() -> None:
    """Commit the updated snapshot file. No-op if not in a git repo or if git is restricted."""
    try:
        result = subprocess.run(
            ["git", "diff", "--quiet", str(SNAPSHOT_PATH)],
            capture_output=True,
        )
        if result.returncode == 0:
            logger.info("Snapshot unchanged — no git commit needed.")
            return

        subprocess.run(["git", "add", str(SNAPSHOT_PATH)], capture_output=True)
        commit_msg = (
            f"chore: update pipeline snapshot "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        )
        result = subprocess.run(
            ["git", "commit", "-m", commit_msg],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            logger.info("Snapshot committed to git.")
        else:
            logger.info("git commit skipped (returncode=%d): %s",
                        result.returncode, result.stderr.strip()[:120])
    except Exception as exc:
        logger.info("git commit skipped in this environment: %s", type(exc).__name__)


# ── Main ──────────────────────────────────────────────────────────────────────

def main(publish: bool = False) -> dict:
    mode = "PUBLISH" if publish else "DRAFT"
    print(f"\n{'═'*60}")
    print(f"  Pipeline Delta Reporter — {mode} mode")
    print(f"{'═'*60}")

    # 1. Load previous snapshot
    prev_snapshot = load_snapshot()
    is_first_run  = prev_snapshot.get("snapshot_date") is None
    print(f"\n  Previous snapshot : "
          f"{prev_snapshot.get('snapshot_date','none (first run)')}")
    print(f"  Previous deals    : {prev_snapshot.get('total_deals', 0)}")
    print(f"  Previous ACV      : ${prev_snapshot.get('total_value', 0):,.2f}")

    # 2. Fetch current state
    print("\n  Fetching current HubSpot deals...")
    deals        = fetch_current_deals()
    curr_snapshot = build_snapshot(deals)
    print(f"  Current deals     : {curr_snapshot['total_deals']}")
    print(f"  Current ACV       : ${curr_snapshot['total_value']:,.2f}")

    # 3. Compute delta
    delta = compute_delta(prev_snapshot, curr_snapshot)
    print(f"\n  Deals added       : {len(delta['added_deals'])}")
    print(f"  Deals removed     : {len(delta['removed_deals'])}")
    print(f"  Stage changes     : {len(delta['stage_changes'])}")
    print(f"  ACV delta         : "
          f"{'+' if delta['acv_delta'] >= 0 else ''}${delta['acv_delta']:,.2f}")

    if publish:
        # -- PUBLISH mode: read approved draft and post to leadership webhook
        leadership_webhook = os.environ.get("SLACK_LEADERSHIP_WEBHOOK_URL", SLACK_WEBHOOK_URL)
        if DRAFT_PATH.exists():
            draft_data = json.loads(DRAFT_PATH.read_text())
            narrative  = draft_data.get("narrative", "")
            delta      = draft_data.get("delta", delta)
        else:
            print("  No draft found — generating fresh narrative.")
            narrative = generate_narrative(delta)

        print("\n  Posting approved narrative to leadership channel...")
        msg = build_slack_message(delta, narrative, is_draft=False)
        # Swap webhook to leadership channel if configured
        original = os.environ.get("SLACK_WEBHOOK_URL")
        if leadership_webhook and leadership_webhook != original:
            import os as _os
            _os.environ["SLACK_WEBHOOK_URL"] = leadership_webhook
        post_to_slack(msg, label="leadership")
        return delta

    # -- DRAFT mode (default)
    print("\n  Generating Groq narrative...")
    narrative = generate_narrative(delta)
    print(f"\n  Narrative:\n  {narrative}\n")

    # Save draft for human review / publish step
    draft = {"delta": delta, "narrative": narrative, "generated_at": curr_snapshot["snapshot_date"]}
    DRAFT_PATH.write_text(json.dumps(draft, indent=2, default=str))
    print(f"  Draft saved to {DRAFT_PATH}")

    # Post draft to Slack
    msg = build_slack_message(delta, narrative, is_draft=True)
    sent = post_to_slack(msg, label="draft")

    # Stage summary table
    print(f"\n  {'Stage':<35} {'Prev':>5} {'Curr':>5} {'Δ':>5}  {'Prev ACV':>14} {'Curr ACV':>14}")
    print("  " + "-" * 85)
    for s in delta["stage_summary"]:
        if s["curr_count"] == 0 and s["prev_count"] == 0:
            continue
        delta_str = f"{'+' if s['count_delta'] >= 0 else ''}{s['count_delta']}"
        print(f"  {s['stage']:<35} {s['prev_count']:>5} {s['curr_count']:>5} "
              f"{delta_str:>5}  ${s['prev_value']:>12,.0f} ${s['curr_value']:>12,.0f}")

    # Save new snapshot and commit
    save_snapshot(curr_snapshot)
    git_commit_snapshot()

    print(f"\n  Slack draft posted : {'yes' if sent else 'no (no webhook)'}")
    print(f"  Snapshot updated   : {SNAPSHOT_PATH}")
    print(f"\n  To post to leadership, re-run with --publish")

    return {"delta": delta, "narrative": narrative}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--publish", action="store_true",
                    help="Post approved draft to leadership channel")
    args = ap.parse_args()
    main(publish=args.publish)
