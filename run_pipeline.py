"""
GTM Intelligence Platform — Full Pipeline Orchestrator
Runs Steps 1-5 for High and Medium tier companies.

Each step saves its output to .pipeline_cache/<step>.json so re-runs are
incremental: completed steps are skipped automatically.

Usage:
  python run_pipeline.py            # run all steps (skipping cached ones)
  python run_pipeline.py --fresh    # clear cache and re-run everything
  python run_pipeline.py --step 2   # run only one specific step
"""

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

import snowflake.connector

from agents.company_resolution import resolve_company_name
from agents.signal_interpreter import interpret_signals
from agents.crm_entry_agent import process_batch
from enrichment.hunter_enricher import domain_search

logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s %(levelname)s %(message)s")

CACHE_DIR   = Path(__file__).parent / ".pipeline_cache"
WORKERS     = 10      # parallel Groq threads
BATCH_SLEEP = 0.2     # seconds between thread batches (rate-limit buffer)
PROGRESS_N  = 10

CACHE_DIR.mkdir(exist_ok=True)


# ── cache helpers ─────────────────────────────────────────────────────────────

def _cache_path(step: int) -> Path:
    return CACHE_DIR / f"step{step}.json"

def _load_cache(step: int):
    p = _cache_path(step)
    if p.exists():
        return json.loads(p.read_text())
    return None

def _save_cache(step: int, data) -> None:
    _cache_path(step).write_text(json.dumps(data, default=str, indent=2))

def _section(title: str) -> None:
    print(f"\n{'═'*70}\n  {title}\n{'═'*70}")


# ── Step 1 — Snowflake ────────────────────────────────────────────────────────

def step1(force: bool = False) -> list[dict]:
    _section("STEP 1 — Query mart_intent_scores  (High + Medium tier)")
    if not force and (cached := _load_cache(1)):
        high = sum(1 for r in cached if r["intent_tier"] == "High")
        med  = sum(1 for r in cached if r["intent_tier"] == "Medium")
        print(f"  [CACHED]  {len(cached):,} companies  |  High: {high}  |  Medium: {med}")
        return cached

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT company_name, total_intent_score, intent_tier,
               total_award_amount, contract_count, filing_count,
               most_recent_filing, state_code, sic_code, ticker, cik
        FROM GTM_INTELLIGENCE.STAGING_marts.mart_intent_scores
        WHERE intent_tier IN ('High','Medium')
        ORDER BY total_intent_score DESC
    """)
    cols = [d[0].lower() for d in cur.description]
    rows = []
    for raw in cur.fetchall():
        r = dict(zip(cols, raw))
        for k in ("total_award_amount",):
            if r.get(k) is not None: r[k] = float(r[k])
        for k in ("total_intent_score", "contract_count", "filing_count"):
            if r.get(k) is not None: r[k] = int(r[k])
        if r.get("most_recent_filing"):
            r["most_recent_filing"] = str(r["most_recent_filing"])
        rows.append(r)
    conn.close()

    high = sum(1 for r in rows if r["intent_tier"] == "High")
    med  = sum(1 for r in rows if r["intent_tier"] == "Medium")
    print(f"  Fetched {len(rows):,} companies  |  High: {high}  |  Medium: {med}")
    print(f"  Score range: {rows[-1]['total_intent_score']} – {rows[0]['total_intent_score']}")
    _save_cache(1, rows)
    return rows


# ── Step 2 — Company Resolution (parallel Groq) ───────────────────────────────

def _resolve_one(company: dict) -> dict:
    resolution = resolve_company_name(company["company_name"])
    return {**company, **resolution, "company_name": company["company_name"]}

def step2(companies: list[dict], force: bool = False) -> list[dict]:
    _section(f"STEP 2 — Company Resolution via Groq  "
             f"({len(companies)} companies, {WORKERS} parallel threads)")
    if not force and (cached := _load_cache(2)):
        with_domain = sum(1 for r in cached if r.get("domain"))
        print(f"  [CACHED]  {len(cached):,} resolved  |  {with_domain} with domain")
        return cached

    results: dict[int, dict] = {}
    start = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_resolve_one, co): i
                   for i, co in enumerate(companies)}
        done = 0
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                results[idx] = {**companies[idx], "domain": None,
                                "canonical_name": companies[idx]["company_name"]}
            done += 1
            if done % PROGRESS_N == 0 or done == len(companies):
                elapsed = time.time() - start
                print(f"  [{done:>4}/{len(companies)}] "
                      f"{done/len(companies)*100:5.1f}%  "
                      f"elapsed {elapsed:.0f}s")

    ordered = [results[i] for i in range(len(companies))]
    with_domain = sum(1 for r in ordered if r.get("domain"))
    print(f"\n  Resolved {len(ordered)}  |  "
          f"{with_domain} with domain  |  "
          f"{len(ordered)-with_domain} without")
    _save_cache(2, ordered)
    return ordered


# ── Step 3 — Hunter.io Enrichment (parallel) ─────────────────────────────────

def _enrich_one(company: dict) -> dict:
    domain = company.get("domain")
    if not domain:
        return {**company, "contacts": [], "best_contact": None}
    contacts = domain_search(domain, limit=10)
    best = _best_contact(contacts)
    return {**company, "contacts": contacts, "best_contact": best}

def _seniority_rank(s: str | None) -> int:
    return {"executive": 4, "director": 3, "manager": 2, "senior": 1}.get(
        (s or "").lower(), 0)

def _best_contact(contacts: list[dict]) -> dict | None:
    if not contacts: return None
    return max(contacts,
               key=lambda c: (_seniority_rank(c.get("seniority")),
                              c.get("confidence") or 0))

def step3(companies: list[dict], force: bool = False) -> list[dict]:
    _section(f"STEP 3 — Hunter.io Contact Enrichment  ({len(companies)} companies)")
    if not force and (cached := _load_cache(3)):
        with_c = sum(1 for r in cached if r.get("best_contact"))
        skipped = sum(1 for r in cached if not r.get("domain"))
        print(f"  [CACHED]  {with_c} with contacts  |  {skipped} skipped (no domain)")
        return cached

    results: dict[int, dict] = {}
    total_contacts = 0
    start = time.time()

    with ThreadPoolExecutor(max_workers=5) as pool:   # Hunter rate limit ~5 rps
        futures = {pool.submit(_enrich_one, co): i
                   for i, co in enumerate(companies)}
        done = 0
        for fut in as_completed(futures):
            idx = futures[fut]
            res = fut.result()
            results[idx] = res
            total_contacts += len(res.get("contacts") or [])
            done += 1
            if done % PROGRESS_N == 0 or done == len(companies):
                elapsed = time.time() - start
                print(f"  [{done:>4}/{len(companies)}]  "
                      f"contacts so far: {total_contacts}  "
                      f"elapsed {elapsed:.0f}s")

    ordered = [results[i] for i in range(len(companies))]
    with_c  = sum(1 for r in ordered if r.get("best_contact"))
    skipped = sum(1 for r in ordered if not r.get("domain"))
    print(f"\n  {with_c} companies with a best contact  |  "
          f"{skipped} skipped  |  {total_contacts} raw contacts total")
    _save_cache(3, ordered)
    return ordered


# ── Step 4 — Signal Interpretation (parallel Groq) ───────────────────────────

def _interpret_one(company: dict) -> dict:
    signal = {k: company.get(k) for k in (
        "company_name", "intent_tier", "total_intent_score",
        "total_award_amount", "contract_count", "filing_count",
        "most_recent_filing", "state_code", "sic_code",
        "industry", "hq_city", "hq_state",
    )}
    interp = interpret_signals(signal)
    interp["company_name"] = company["company_name"]
    return interp

def step4(companies: list[dict], force: bool = False) -> list[dict]:
    _section(f"STEP 4 — Signal Interpretation via Groq  ({len(companies)} companies)")
    if not force and (cached := _load_cache(4)):
        by_action = {}
        for r in cached:
            a = r.get("recommended_action","Unknown")
            by_action[a] = by_action.get(a,0)+1
        print(f"  [CACHED]  {len(cached)} interpreted")
        print(f"  Actions: {dict(sorted(by_action.items()))}")
        return cached

    results: dict[int, dict] = {}
    start = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_interpret_one, co): i
                   for i, co in enumerate(companies)}
        done = 0
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                results[idx] = {
                    "company_name": companies[idx]["company_name"],
                    "summary": "Error during interpretation.",
                    "recommended_action": "Monitor",
                    "talking_points": [],
                    "urgency": "Low",
                }
            done += 1
            if done % PROGRESS_N == 0 or done == len(companies):
                elapsed = time.time() - start
                print(f"  [{done:>4}/{len(companies)}] "
                      f"{done/len(companies)*100:5.1f}%  "
                      f"elapsed {elapsed:.0f}s")

    ordered = [results[i] for i in range(len(companies))]
    by_action: dict[str, int] = {}
    by_urgency: dict[str, int] = {}
    for r in ordered:
        a = r.get("recommended_action","Unknown")
        u = r.get("urgency","Unknown")
        by_action[a]  = by_action.get(a,0)+1
        by_urgency[u] = by_urgency.get(u,0)+1

    print(f"\n  Actions : {dict(sorted(by_action.items()))}")
    print(f"  Urgency : {dict(sorted(by_urgency.items()))}")
    _save_cache(4, ordered)
    return ordered


# ── Step 5 — HubSpot CRM upsert (parallel + resumable) ───────────────────────

HS_WORKERS      = 8           # parallel HubSpot threads
PROGRESS_FILE   = CACHE_DIR / "step5_progress.json"


def _load_progress() -> dict:
    """Load partial-progress dict {company_name: action} from disk."""
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {}


def _save_progress(progress: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(progress))


def _upsert_one(args: tuple) -> tuple[str, str]:
    """Worker: upsert a single company; returns (company_name, action)."""
    from agents.crm_entry_agent import create_or_update_company, add_note
    company, interpretation = args
    company_id, action = create_or_update_company(company, interpretation)
    if company_id and interpretation.get("talking_points"):
        bc = company.get("best_contact") or {}
        contact_line = ""
        if bc.get("email"):
            contact_line = (
                f"\nBest Contact: {bc.get('first_name','')} "
                f"{bc.get('last_name','')} "
                f"({bc.get('position','')}) — {bc.get('email')}\n"
            )
        talking_points = "\n".join(f"- {tp}" for tp in interpretation["talking_points"])
        note = (
            f"GTM Intelligence Update\n"
            f"Intent Score : {company.get('total_intent_score','N/A')} "
            f"({company.get('intent_tier','N/A')})\n"
            f"Urgency      : {interpretation.get('urgency','N/A')}\n"
            f"Action       : {interpretation.get('recommended_action')}\n"
            f"{contact_line}\n"
            f"Talking Points:\n{talking_points}"
        )
        add_note(company_id, note)
    return company["company_name"], action


def step5(companies: list[dict], interpretations: list[dict],
          force: bool = False) -> None:
    _section(f"STEP 5 — HubSpot CRM Upsert  ({len(companies)} companies, "
             f"{HS_WORKERS} parallel threads)")

    if not force and _cache_path(5).exists():
        cached = json.loads(_cache_path(5).read_text())
        print(f"  [CACHED]  created: {cached['created']}  "
              f"updated: {cached['updated']}  failed: {cached['failed']}")
        return

    # Resume support: skip companies already pushed in a previous partial run
    progress = {} if force else _load_progress()
    if progress:
        already = len(progress)
        print(f"  Resuming — {already} already pushed, "
              f"{len(companies)-already} remaining")

    interp_map = {i["company_name"]: i for i in interpretations}

    # Build work queue — only companies not yet in progress
    pending = [
        (co, interp_map.get(co["company_name"], {}))
        for co in companies
        if co["company_name"] not in progress
    ]

    totals = {
        "created": sum(1 for v in progress.values() if v == "created"),
        "updated": sum(1 for v in progress.values() if v == "updated"),
        "failed":  sum(1 for v in progress.values() if v == "failed"),
    }

    done_count = len(progress)
    total      = len(companies)

    with ThreadPoolExecutor(max_workers=HS_WORKERS) as pool:
        futures = {pool.submit(_upsert_one, args): args[0]["company_name"]
                   for args in pending}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                _, action = fut.result()
            except Exception:
                action = "failed"
            progress[name] = action
            totals[action] = totals.get(action, 0) + 1
            done_count += 1

            # Save incremental progress every 25 completions
            if done_count % 25 == 0 or done_count == total:
                _save_progress(progress)
                print(f"  [{done_count:>4}/{total}]  "
                      f"created: {totals['created']}  "
                      f"updated: {totals['updated']}  "
                      f"failed:  {totals['failed']}")

    # All done — write final summary cache and clean up progress file
    _save_cache(5, totals)
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()

    print(f"\n  ── Final HubSpot counts ──────────────────")
    print(f"  Created  : {totals['created']}")
    print(f"  Updated  : {totals['updated']}")
    print(f"  Failed   : {totals['failed']}")
    print(f"  Total    : {sum(totals.values())}")


# ── Sample output ─────────────────────────────────────────────────────────────

def print_sample(companies: list[dict], interpretations: list[dict],
                 n: int = 5) -> None:
    _section(f"SAMPLE OUTPUT — first {n} companies")
    interp_map = {i["company_name"]: i for i in interpretations}
    for idx, co in enumerate(companies[:n], 1):
        interp = interp_map.get(co["company_name"], {})
        bc = co.get("best_contact") or {}
        print(f"\n  [{idx}] {co.get('canonical_name') or co['company_name']}")
        print(f"       Tier/Score : {co['intent_tier']} / {co['total_intent_score']}")
        print(f"       Domain     : {co.get('domain') or '—'}")
        print(f"       Industry   : {co.get('industry') or '—'}")
        print(f"       HQ         : {co.get('hq_city') or '—'}, "
              f"{co.get('hq_state') or '—'}")
        if bc:
            print(f"       Contact    : {bc.get('first_name','')} "
                  f"{bc.get('last_name','')} "
                  f"({bc.get('position','')}) — {bc.get('email','')}")
        else:
            print(f"       Contact    : —")
        print(f"       Urgency    : {interp.get('urgency','—')}")
        print(f"       Action     : {interp.get('recommended_action','—')}")
        for tp in (interp.get("talking_points") or [])[:2]:
            print(f"         • {tp}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fresh",  action="store_true",
                    help="Clear cache and rerun all steps")
    ap.add_argument("--step", type=int, choices=[1,2,3,4,5],
                    help="Run only this step (uses cached output from prior steps)")
    args = ap.parse_args()

    if args.fresh:
        for p in CACHE_DIR.glob("step*.json"):
            p.unlink()
        print("  Cache cleared.")

    wall = time.time()
    print("\n" + "█"*70)
    print("  GTM INTELLIGENCE PIPELINE — HIGH + MEDIUM TIER COMPANIES")
    print("█"*70)

    only = args.step

    companies       = step1(force=(only==1 and args.fresh) or (args.fresh))
    if only and only < 2: return
    resolved        = step2(companies,    force=only==2 or args.fresh)
    if only and only < 3: return
    enriched        = step3(resolved,     force=only==3 or args.fresh)
    if only and only < 4: return
    interpretations = step4(enriched,     force=only==4 or args.fresh)
    if only and only < 5: return
    step5(enriched, interpretations,      force=only==5 or args.fresh)
    print_sample(enriched, interpretations)

    _section(f"PIPELINE COMPLETE  —  {time.time()-wall:.0f}s total")


if __name__ == "__main__":
    main()
