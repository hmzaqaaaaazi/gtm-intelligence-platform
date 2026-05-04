"""
MQL Compliance Checker
- Fetches all contacts with lifecyclestage = marketingqualifiedlead
- Flags any that have had no logged activity in the last 24 hours
- Sends a Slack alert per violation with contact name, company,
  time since MQL, assigned owner, and a direct HubSpot link
- Appends results to compliance/mql_log.json
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
ACTIVITY_WINDOW_H = 24        # hours — flag contacts with no activity beyond this
LOG_PATH          = Path(__file__).parent / "mql_log.json"


# ── HubSpot helpers ────────────────────────────────────────────────────────────

def _hs_headers() -> dict:
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        raise EnvironmentError("HUBSPOT_ACCESS_TOKEN not set.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _resolve_owner_name(owner_id: str | None) -> str:
    """Attempt to resolve owner ID → display name via HubSpot Owners API."""
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


def _contact_link(contact_id: str) -> str:
    return f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/contact/{contact_id}"


# ── Step 1 — Fetch MQL contacts ───────────────────────────────────────────────

def fetch_mqls() -> list[dict]:
    """Return all contacts currently in the marketingqualifiedlead lifecycle stage."""
    url  = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search"
    after = None
    all_contacts: list[dict] = []

    while True:
        payload: dict = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "lifecyclestage",
                    "operator":     "EQ",
                    "value":        "marketingqualifiedlead",
                }]
            }],
            "properties": [
                "firstname", "lastname", "email", "company",
                "hubspot_owner_id",
                "hs_lastcontacted",           # last time a call/email was logged
                "notes_last_updated",          # last note timestamp
                "hs_lifecyclestage_marketingqualifiedlead_date",
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
            logger.error("Failed to fetch MQL contacts: %s", exc)
            break

        all_contacts.extend(body.get("results", []))
        paging = body.get("paging", {}).get("next", {})
        after  = paging.get("after")
        if not after:
            break

    logger.info("Fetched %d MQL contacts.", len(all_contacts))
    return all_contacts


# ── Step 2 — Identify non-compliant MQLs ─────────────────────────────────────

def _last_activity_dt(props: dict) -> datetime | None:
    """Return the most recent activity timestamp across all tracked fields."""
    candidates = [
        props.get("hs_lastcontacted"),
        props.get("notes_last_updated"),
    ]
    latest: datetime | None = None
    for raw in candidates:
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if latest is None or dt > latest:
                latest = dt
        except Exception:
            pass
    return latest


def _mql_since_dt(props: dict) -> datetime | None:
    """Return when this contact was first set to MQL."""
    raw = props.get("hs_lifecyclestage_marketingqualifiedlead_date")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def check_compliance(contacts: list[dict]) -> list[dict]:
    """
    Return contacts that have had no activity in the last ACTIVITY_WINDOW_H hours.
    Each entry includes: contact_id, name, email, company, owner_name,
    hours_since_activity, hours_since_mql, hubspot_link, issues.
    """
    cutoff    = datetime.now(timezone.utc) - timedelta(hours=ACTIVITY_WINDOW_H)
    now       = datetime.now(timezone.utc)
    violations: list[dict] = []

    for contact in contacts:
        cid   = contact["id"]
        props = contact.get("properties", {})
        last_activity = _last_activity_dt(props)
        mql_since     = _mql_since_dt(props)

        # Non-compliant if last activity is missing OR older than the cutoff
        if last_activity is None or last_activity < cutoff:
            hours_since_activity = (
                round((now - last_activity).total_seconds() / 3600, 1)
                if last_activity else None
            )
            hours_since_mql = (
                round((now - mql_since).total_seconds() / 3600, 1)
                if mql_since else None
            )
            name = " ".join(filter(None, [
                props.get("firstname"), props.get("lastname")
            ])) or "Unknown"

            violations.append({
                "contact_id":          cid,
                "name":                name,
                "email":               props.get("email") or "N/A",
                "company":             props.get("company") or "N/A",
                "owner_id":            props.get("hubspot_owner_id"),
                "hours_since_activity": hours_since_activity,
                "hours_since_mql":     hours_since_mql,
                "hubspot_link":        _contact_link(cid),
                "checked_at":          now.isoformat(),
            })

    logger.info(
        "Compliance check: %d/%d MQLs non-compliant (no activity in %dh)",
        len(violations), len(contacts), ACTIVITY_WINDOW_H,
    )
    return violations


# ── Step 3 — Resolve owner names (batch) ──────────────────────────────────────

def enrich_with_owner_names(violations: list[dict]) -> list[dict]:
    """Resolve owner IDs → display names, caching per unique ID."""
    cache: dict[str, str] = {}
    for v in violations:
        oid = v.get("owner_id")
        if oid and oid not in cache:
            cache[oid] = _resolve_owner_name(oid)
        v["owner_name"] = cache.get(oid, "Unassigned") if oid else "Unassigned"
    return violations


# ── Step 4 — Slack alert ──────────────────────────────────────────────────────

def send_slack_alert(violations: list[dict]) -> None:
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping Slack alert.")
        return
    if not violations:
        logger.info("All MQLs are compliant. No Slack alert sent.")
        return

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f":rotating_light: *MQL Compliance Alert — {date_str}*",
        f"*{len(violations)} MQL(s) have had no activity in the last "
        f"{ACTIVITY_WINDOW_H} hours:*\n",
    ]

    for v in violations[:15]:
        since_str = (
            f"{v['hours_since_activity']:.0f}h ago"
            if v["hours_since_activity"] is not None
            else "never"
        )
        mql_str = (
            f"{v['hours_since_mql']:.0f}h"
            if v["hours_since_mql"] is not None
            else "unknown"
        )
        lines.append(
            f"• *{v['name']}* ({v['company']}) — {v['email']}\n"
            f"  Last activity: {since_str} | MQL for: {mql_str} | "
            f"Owner: {v.get('owner_name','Unassigned')} | "
            f"<{v['hubspot_link']}|View in HubSpot>"
        )

    if len(violations) > 15:
        lines.append(f"\n_...and {len(violations) - 15} more._")

    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": "\n".join(lines)}, timeout=10)
        r.raise_for_status()
        logger.info("Slack MQL alert sent (%d violations).", len(violations))
    except Exception as exc:
        logger.error("Failed to send Slack alert: %s", exc)


# ── Step 5 — Log to JSON ──────────────────────────────────────────────────────

def log_results(violations: list[dict], total_mqls: int) -> None:
    """Append this run's results to compliance/mql_log.json."""
    entry = {
        "run_at":       datetime.now(timezone.utc).isoformat(),
        "total_mqls":   total_mqls,
        "violations":   len(violations),
        "details":      violations,
    }
    history: list[dict] = []
    if LOG_PATH.exists():
        try:
            history = json.loads(LOG_PATH.read_text())
        except Exception:
            history = []

    history.append(entry)
    # Keep last 90 runs
    history = history[-90:]
    LOG_PATH.write_text(json.dumps(history, indent=2, default=str))
    logger.info("Results logged to %s", LOG_PATH)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> list[dict]:
    print(f"Running MQL compliance check  "
          f"(window: {ACTIVITY_WINDOW_H}h, portal: {HUBSPOT_PORTAL_ID})")
    contacts   = fetch_mqls()
    violations = check_compliance(contacts)
    violations = enrich_with_owner_names(violations)
    send_slack_alert(violations)
    log_results(violations, len(contacts))

    print(f"\nTotal MQLs checked : {len(contacts)}")
    print(f"Violations found   : {len(violations)}")
    if violations:
        print("\nNon-compliant MQLs:")
        for v in violations:
            print(f"  {v['name']:<30} {v['company']:<25} "
                  f"last_activity={v['hours_since_activity']}h  "
                  f"owner={v.get('owner_name','?')}")
    return violations


if __name__ == "__main__":
    main()
