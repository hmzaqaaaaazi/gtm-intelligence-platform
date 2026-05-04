"""
create_deals.py
- Reads .pipeline_cache/step4.json  → 9 Immediate Outreach companies
- Reads .pipeline_cache/step2.json  → intent scores + total_award_amount + canonical_name
- For each company: looks up HubSpot company ID, creates a Deal, associates it
- Deal properties:
    dealname  : "{canonical_name} — GTM Intelligence"
    pipeline  : default
    dealstage : appointmentscheduled
    amount    : total_award_amount * 0.001  (0.1% of federal contract value)
    closedate : 90 days from today  (ms epoch)
    description: "High intent account identified by GTM Intelligence Pipeline.
                  Intent score: {score}/100"
- After all deals are created, runs pipeline_delta.py to capture baseline snapshot
"""

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HUBSPOT_BASE_URL = "https://api.hubapi.com"
CACHE_DIR        = Path(__file__).parent.parent / ".pipeline_cache"
STEP2_PATH       = CACHE_DIR / "step2.json"
STEP4_PATH       = CACHE_DIR / "step4.json"


# ── HubSpot helpers ────────────────────────────────────────────────────────────

def _hs_headers() -> dict:
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        raise EnvironmentError("HUBSPOT_ACCESS_TOKEN not set.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _hs_post(url: str, payload: dict, retries: int = 4) -> requests.Response:
    """POST with exponential back-off on 429."""
    delay = 1.0
    for attempt in range(retries):
        r = requests.post(url, json=payload, headers=_hs_headers(), timeout=30)
        if r.status_code == 429:
            wait = float(r.headers.get("Retry-After", delay))
            logger.warning("Rate-limited — waiting %.1fs", wait)
            time.sleep(wait)
            delay *= 2
            continue
        return r
    return r   # return last response even if still 429


# ── Step 1 — Load cache ───────────────────────────────────────────────────────

def load_immediate_outreach() -> list[dict]:
    """
    Returns a list of dicts with keys from step2 (scores, awards, canonical name)
    for every company whose step4 recommended_action == 'Immediate Outreach'.
    """
    step4: list[dict] = json.loads(STEP4_PATH.read_text())
    step2: list[dict] = json.loads(STEP2_PATH.read_text())

    outreach_names = {
        c["company_name"].upper()
        for c in step4
        if c.get("recommended_action") == "Immediate Outreach"
    }
    logger.info("Immediate Outreach companies in step4: %d", len(outreach_names))

    # Index step2 by upper-cased company_name for fast lookup
    step2_index = {c["company_name"].upper(): c for c in step2}

    results: list[dict] = []
    for raw_name in outreach_names:
        s2 = step2_index.get(raw_name)
        if not s2:
            logger.warning("No step2 record for %s — skipping", raw_name)
            continue
        results.append(s2)

    logger.info("Matched %d companies in step2", len(results))
    return results


# ── Step 2 — Look up HubSpot company IDs ──────────────────────────────────────

def find_hubspot_company_id(canonical_name: str) -> str | None:
    """Search HubSpot for a company by name and return its ID."""
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/companies/search"
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "name",
                "operator":     "EQ",
                "value":        canonical_name,
            }]
        }],
        "properties": ["name", "domain"],
        "limit": 1,
    }
    try:
        r = _hs_post(url, payload)
        if r.status_code == 200:
            results = r.json().get("results", [])
            if results:
                return results[0]["id"]
        logger.warning("Company not found in HubSpot: %s (status %d)", canonical_name, r.status_code)
    except Exception as exc:
        logger.error("Error searching for company %s: %s", canonical_name, exc)
    return None


# ── Step 3 — Create Deal ──────────────────────────────────────────────────────

def _closedate_ms() -> int:
    """Return epoch-milliseconds for 90 days from today (HubSpot date format)."""
    close_dt = datetime.now(timezone.utc) + timedelta(days=90)
    # HubSpot wants midnight UTC in ms
    midnight  = close_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(midnight.timestamp() * 1000)


def create_deal(company: dict) -> dict | None:
    """
    Create a HubSpot Deal for the given company record (from step2).
    Returns the created deal dict or None on failure.
    """
    canonical_name  = company.get("canonical_name") or company["company_name"]
    score           = company.get("total_intent_score", 0) or 0
    award           = float(company.get("total_award_amount") or 0)
    acv             = round(award * 0.001, 2)   # 0.1% proxy

    deal_name   = f"{canonical_name} — GTM Intelligence"
    description = (
        f"High intent account identified by GTM Intelligence Pipeline. "
        f"Intent score: {score}/100. "
        f"Federal contract award base: ${award:,.0f}."
    )

    payload = {
        "properties": {
            "dealname":    deal_name,
            "pipeline":    "default",
            "dealstage":   "appointmentscheduled",
            "amount":      str(acv),
            "closedate":   str(_closedate_ms()),
            "description": description,
        }
    }

    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals"
    try:
        r = requests.post(url, json=payload, headers=_hs_headers(), timeout=30)
        if r.status_code == 429:
            wait = float(r.headers.get("Retry-After", 5))
            time.sleep(wait)
            r = requests.post(url, json=payload, headers=_hs_headers(), timeout=30)

        if r.status_code in (200, 201):
            deal = r.json()
            logger.info("Created deal: %s  (id=%s)", deal_name, deal["id"])
            return deal
        else:
            logger.error("Failed to create deal for %s: %d %s",
                         canonical_name, r.status_code, r.text[:200])
    except Exception as exc:
        logger.error("Exception creating deal for %s: %s", canonical_name, exc)
    return None


# ── Step 4 — Associate Deal ↔ Company ─────────────────────────────────────────

def associate_deal_with_company(deal_id: str, company_id: str) -> bool:
    """
    Create a DEAL → COMPANY association using HubSpot's association API v4.
    Association type 5 = Deal-to-Company (default).
    """
    url = (
        f"{HUBSPOT_BASE_URL}/crm/v4/objects/deals/{deal_id}"
        f"/associations/companies/{company_id}"
    )
    payload = [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 5}]
    try:
        r = requests.put(url, json=payload, headers=_hs_headers(), timeout=15)
        if r.status_code in (200, 201):
            logger.info("Associated deal %s → company %s", deal_id, company_id)
            return True
        logger.warning("Association failed (%d): %s", r.status_code, r.text[:150])
    except Exception as exc:
        logger.error("Exception associating deal %s → company %s: %s",
                     deal_id, company_id, exc)
    return False


# ── Step 5 — Run pipeline_delta.py ───────────────────────────────────────────

def run_pipeline_delta() -> None:
    print("\n" + "═" * 60)
    print("  Running pipeline_delta.py to capture baseline…")
    print("═" * 60 + "\n")
    result = subprocess.run(
        [sys.executable, "-m", "compliance.pipeline_delta"],
        capture_output=False,
    )
    if result.returncode != 0:
        logger.warning("pipeline_delta.py exited with code %d", result.returncode)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> list[dict]:
    print("═" * 60)
    print("  Create Deals — Immediate Outreach Companies")
    print("═" * 60)

    # Load the 9 Immediate Outreach companies from cache
    companies = load_immediate_outreach()
    print(f"\n  {len(companies)} Immediate Outreach companies loaded from cache\n")

    created_deals: list[dict] = []
    close_date_str = (datetime.now(timezone.utc) + timedelta(days=90)).strftime("%Y-%m-%d")

    for company in companies:
        canonical_name = company.get("canonical_name") or company["company_name"]
        score          = company.get("total_intent_score", 0) or 0
        award          = float(company.get("total_award_amount") or 0)
        acv            = round(award * 0.001, 2)

        print(f"  ── {canonical_name}")
        print(f"     Intent score : {score}/100")
        print(f"     Federal award: ${award:>18,.0f}")
        print(f"     Est. ACV     : ${acv:>18,.2f}  (0.1% of award)")

        # Find the existing HubSpot company record
        company_id = find_hubspot_company_id(canonical_name)
        if company_id:
            print(f"     HubSpot co.  : {company_id}")
        else:
            print(f"     HubSpot co.  : NOT FOUND — deal will be created unassociated")

        # Create the deal
        deal = create_deal(company)
        if not deal:
            print(f"     Status       : ✗  FAILED to create deal\n")
            continue

        deal_id   = deal["id"]
        deal_name = deal["properties"]["dealname"]
        print(f"     Deal ID      : {deal_id}")
        print(f"     Deal name    : {deal_name}")

        # Associate with the company
        assoc_ok = False
        if company_id:
            assoc_ok = associate_deal_with_company(deal_id, company_id)

        print(f"     Associated   : {'✓' if assoc_ok else '✗ (no company ID found)'}")
        print(f"     Close date   : {close_date_str}")
        print(f"     Status       : ✓  CREATED\n")

        created_deals.append({
            "deal_id":      deal_id,
            "deal_name":    deal_name,
            "acv":          acv,
            "company_id":   company_id,
            "company_name": canonical_name,
            "score":        score,
            "award":        award,
            "associated":   assoc_ok,
        })

        # Brief pause to respect rate limits
        time.sleep(0.15)

    # Summary
    print("─" * 60)
    print(f"  Created : {len(created_deals)}/{len(companies)} deals")
    total_acv = sum(d["acv"] for d in created_deals)
    print(f"  Total pipeline ACV created : ${total_acv:,.2f}")
    print()

    if created_deals:
        print(f"  {'Company':<40} {'ACV':>14}  {'Associated'}")
        print("  " + "-" * 65)
        for d in sorted(created_deals, key=lambda x: x["acv"], reverse=True):
            print(f"  {d['company_name'][:38]:<40} ${d['acv']:>12,.2f}  "
                  f"{'✓' if d['associated'] else '✗'}")

    # Run pipeline_delta to baseline the new deals
    run_pipeline_delta()

    return created_deals


if __name__ == "__main__":
    main()
