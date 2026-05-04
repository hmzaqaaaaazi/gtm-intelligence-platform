"""
Stale Deals Checker
- Fetches all open HubSpot deals (not Closed Won / Closed Lost)
- Flags any deal with no activity in the last 14 days
- Sends a Slack alert with deal name, stage, ACV, days stale, assigned rep
- Appends results to compliance/stale_deals_log.json
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HUBSPOT_BASE_URL  = "https://api.hubapi.com"
HUBSPOT_PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "244783142")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
STALE_DAYS        = 14
LOG_PATH          = Path(__file__).parent / "stale_deals_log.json"

# HubSpot default pipeline closed stages
CLOSED_STAGES = {"closedwon", "closedlost"}

# Human-readable stage labels
STAGE_LABELS = {
    "appointmentscheduled":   "Appointment Scheduled",
    "qualifiedtobuy":         "Qualified to Buy",
    "presentationscheduled":  "Presentation Scheduled",
    "decisionmakerboughtin":  "Decision Maker Bought-In",
    "contractsent":           "Contract Sent",
    "closedwon":              "Closed Won",
    "closedlost":             "Closed Lost",
}


# ── HubSpot helpers ────────────────────────────────────────────────────────────

def _hs_headers() -> dict:
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        raise EnvironmentError("HUBSPOT_ACCESS_TOKEN not set.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _resolve_owner_name(owner_id: str | None) -> str:
    if not owner_id:
        return "Unassigned"
    try:
        r = requests.get(
            f"{HUBSPOT_BASE_URL}/crm/v3/owners/{owner_id}",
            headers=_hs_headers(), timeout=10,
        )
        if r.status_code == 200:
            d = r.json()
            return f"{d.get('firstName','')} {d.get('lastName','')}".strip() or owner_id
    except Exception:
        pass
    return f"Owner#{owner_id}"


def _deal_link(deal_id: str) -> str:
    return f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/deal/{deal_id}"


# ── Step 1 — Fetch open deals (paginated) ─────────────────────────────────────

def fetch_open_deals() -> list[dict]:
    """Return all deals NOT in a closed stage."""
    url   = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/search"
    after = None
    all_deals: list[dict] = []

    while True:
        payload: dict = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_is_closed",
                    "operator":     "EQ",
                    "value":        "false",
                }]
            }],
            "properties": [
                "dealname", "amount", "dealstage",
                "hs_lastmodifieddate",
                "notes_last_updated",
                "closedate",
                "hubspot_owner_id",
                "pipeline",
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

        results = body.get("results", [])
        # Extra guard: skip any that slipped through as closed
        for deal in results:
            stage = deal.get("properties", {}).get("dealstage", "")
            if stage not in CLOSED_STAGES:
                all_deals.append(deal)

        paging = body.get("paging", {}).get("next", {})
        after  = paging.get("after")
        if not after:
            break

    logger.info("Fetched %d open deals.", len(all_deals))
    return all_deals


# ── Step 2 — Flag stale deals ─────────────────────────────────────────────────

def find_stale_deals(deals: list[dict]) -> list[dict]:
    """Return deals with no activity (modification or note) in STALE_DAYS days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)
    now    = datetime.now(timezone.utc)
    stale: list[dict] = []

    for deal in deals:
        props = deal.get("properties", {})

        # Use the most recent of last-modified or last-note timestamp
        candidates = [
            props.get("hs_lastmodifieddate"),
            props.get("notes_last_updated"),
        ]
        latest_dt: datetime | None = None
        for raw in candidates:
            if not raw:
                continue
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if latest_dt is None or dt > latest_dt:
                    latest_dt = dt
            except Exception:
                pass

        if latest_dt is None or latest_dt < cutoff:
            days_stale = (
                round((now - latest_dt).days)
                if latest_dt else None
            )
            stage_key   = props.get("dealstage", "unknown")
            stage_label = STAGE_LABELS.get(stage_key, stage_key)

            try:
                acv = float(props.get("amount") or 0)
            except (ValueError, TypeError):
                acv = 0.0

            stale.append({
                "deal_id":     deal["id"],
                "deal_name":   props.get("dealname") or "Unnamed Deal",
                "stage":       stage_label,
                "stage_key":   stage_key,
                "acv":         acv,
                "days_stale":  days_stale,
                "last_activity": latest_dt.isoformat() if latest_dt else None,
                "close_date":  props.get("closedate"),
                "owner_id":    props.get("hubspot_owner_id"),
                "hubspot_link": _deal_link(deal["id"]),
                "checked_at":  now.isoformat(),
            })

    logger.info("Found %d stale deals (>%d days).", len(stale), STALE_DAYS)
    return stale


# ── Step 3 — Resolve owner names ──────────────────────────────────────────────

def enrich_with_owner_names(stale: list[dict]) -> list[dict]:
    cache: dict[str, str] = {}
    for d in stale:
        oid = d.get("owner_id")
        if oid and oid not in cache:
            cache[oid] = _resolve_owner_name(oid)
        d["assigned_rep"] = cache.get(oid, "Unassigned") if oid else "Unassigned"
    return stale


# ── Step 4 — Slack alert ──────────────────────────────────────────────────────

def send_slack_alert(stale: list[dict], total_open: int) -> None:
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping Slack alert.")
        return
    if not stale:
        logger.info("No stale deals found. No alert sent.")
        return

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f":hourglass_flowing_sand: *Stale Deals Alert — {date_str}*",
        f"*{len(stale)} of {total_open} open deal(s) have had "
        f"no activity in the last {STALE_DAYS} days:*\n",
    ]

    # Sort by most stale first
    sorted_stale = sorted(stale, key=lambda d: d["days_stale"] or 0, reverse=True)

    for d in sorted_stale[:15]:
        acv_str   = f"${d['acv']:,.0f}" if d["acv"] else "No ACV"
        days_str  = f"{d['days_stale']}d stale" if d["days_stale"] is not None else "stale (unknown)"
        lines.append(
            f"• *{d['deal_name']}* — {d['stage']}\n"
            f"  ACV: {acv_str} | {days_str} | "
            f"Rep: {d.get('assigned_rep','Unassigned')} | "
            f"<{d['hubspot_link']}|View Deal>"
        )

    if len(stale) > 15:
        lines.append(f"\n_...and {len(stale) - 15} more._")

    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": "\n".join(lines)}, timeout=10)
        r.raise_for_status()
        logger.info("Stale deals Slack alert sent (%d deals).", len(stale))
    except Exception as exc:
        logger.error("Failed to send Slack alert: %s", exc)


# ── Step 5 — Log to JSON ──────────────────────────────────────────────────────

def log_results(stale: list[dict], total_open: int) -> None:
    entry = {
        "run_at":     datetime.now(timezone.utc).isoformat(),
        "total_open": total_open,
        "stale":      len(stale),
        "details":    stale,
    }
    history: list[dict] = []
    if LOG_PATH.exists():
        try:
            history = json.loads(LOG_PATH.read_text())
        except Exception:
            history = []
    history.append(entry)
    history = history[-90:]
    LOG_PATH.write_text(json.dumps(history, indent=2, default=str))
    logger.info("Results logged to %s", LOG_PATH)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> list[dict]:
    print(f"Stale Deals Check  (threshold: {STALE_DAYS} days)")
    deals = fetch_open_deals()
    stale = find_stale_deals(deals)
    stale = enrich_with_owner_names(stale)
    send_slack_alert(stale, len(deals))
    log_results(stale, len(deals))

    print(f"\nOpen deals checked : {len(deals)}")
    print(f"Stale deals found  : {len(stale)}")
    if stale:
        print(f"\n{'Deal Name':<40} {'Stage':<30} {'ACV':>12} {'Days Stale':>10} {'Rep'}")
        print("-" * 100)
        for d in sorted(stale, key=lambda x: x["days_stale"] or 0, reverse=True):
            print(f"  {d['deal_name'][:38]:<40} {d['stage'][:28]:<30} "
                  f"${d['acv']:>10,.0f} {str(d['days_stale'] or '?'):>10} "
                  f"  {d.get('assigned_rep','?')}")
    return stale


if __name__ == "__main__":
    main()
