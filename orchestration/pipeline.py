import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

from ingestion.usaspending import fetch_all_awards
from ingestion.sec_edgar import fetch_all_filings
from ingestion.bls_jobs import fetch_bls_data
from ingestion.snowflake_loader import load_usaspending, load_sec_filings, load_bls_jobs
from agents.company_resolution import resolve_batch
from agents.signal_interpreter import interpret_batch
from agents.crm_entry_agent import process_batch
from enrichment.hunter_enricher import enrich_companies

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def test_connections() -> None:
    """
    Validate all required credentials and connections at startup.
    Prints a status line for each check and exits with code 1 if any fail.
    """
    print("\n=== Running startup connection checks ===")
    failures = []

    # --- Snowflake ---
    required_snowflake = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing_sf = [v for v in required_snowflake if not os.environ.get(v)]
    if missing_sf:
        msg = f"[FAIL] Snowflake: missing env vars: {', '.join(missing_sf)}"
        print(msg)
        failures.append(msg)
    else:
        try:
            import snowflake.connector
            conn = snowflake.connector.connect(
                account=os.environ["SNOWFLAKE_ACCOUNT"],
                user=os.environ["SNOWFLAKE_USER"],
                password=os.environ["SNOWFLAKE_PASSWORD"],
                database=os.getenv("SNOWFLAKE_DATABASE", "GTM_INTELLIGENCE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
                login_timeout=15,
            )
            conn.cursor().execute("SELECT 1")
            conn.close()
            print("[OK]   Snowflake: connected successfully")
        except Exception as exc:
            msg = f"[FAIL] Snowflake: connection failed — {exc}"
            print(msg)
            failures.append(msg)

    # --- Groq ---
    if not os.environ.get("GROQ_API_KEY"):
        msg = "[FAIL] GROQ_API_KEY: not set"
        print(msg)
        failures.append(msg)
    else:
        print("[OK]   GROQ_API_KEY: set")

    # --- HubSpot ---
    if not os.environ.get("HUBSPOT_ACCESS_TOKEN"):
        msg = "[FAIL] HUBSPOT_ACCESS_TOKEN: not set"
        print(msg)
        failures.append(msg)
    else:
        print("[OK]   HUBSPOT_ACCESS_TOKEN: set")

    # --- Hunter ---
    if not os.environ.get("HUNTER_API_KEY"):
        msg = "[FAIL] HUNTER_API_KEY: not set"
        print(msg)
        failures.append(msg)
    else:
        print("[OK]   HUNTER_API_KEY: set")

    # --- Slack ---
    if not os.environ.get("SLACK_WEBHOOK_URL"):
        msg = "[FAIL] SLACK_WEBHOOK_URL: not set"
        print(msg)
        failures.append(msg)
    else:
        print("[OK]   SLACK_WEBHOOK_URL: set")

    print("=========================================\n")

    if failures:
        print(f"ERROR: {len(failures)} check(s) failed. Fix the above issues before running the pipeline.")
        sys.exit(1)

    print("All checks passed. Starting pipeline...\n")


def run_ingestion() -> tuple[list[dict], list[dict], list[dict]]:
    logger.info("=== Step 1: Ingesting data from federal sources ===")

    logger.info("Fetching USASpending awards...")
    usaspending_records = fetch_all_awards()
    logger.info("USASpending: %d records", len(usaspending_records))

    logger.info("Fetching SEC EDGAR 8-K filings...")
    sec_records = fetch_all_filings()
    logger.info("SEC EDGAR: %d records", len(sec_records))

    logger.info("Fetching BLS job openings...")
    bls_records = fetch_bls_data()
    logger.info("BLS: %d records", len(bls_records))

    return usaspending_records, sec_records, bls_records


def run_snowflake_load(usaspending_records, sec_records, bls_records) -> None:
    logger.info("=== Step 2: Loading data into Snowflake ===")
    load_usaspending(usaspending_records)
    load_sec_filings(sec_records)
    load_bls_jobs(bls_records)
    logger.info("Snowflake load complete.")


def run_company_resolution(sec_records: list[dict]) -> list[dict]:
    logger.info("=== Step 3: Resolving company names ===")
    names = [r["company_name"] for r in sec_records if r.get("company_name")]
    resolved = resolve_batch(names)
    logger.info("Resolved %d companies.", len(resolved))
    return resolved


def run_enrichment(companies: list[dict]) -> list[dict]:
    logger.info("=== Step 4: Enriching companies with Hunter.io contacts ===")
    enriched = enrich_companies(companies)
    logger.info("Enrichment complete for %d companies.", len(enriched))
    return enriched


def run_signal_interpretation(companies: list[dict]) -> list[dict]:
    logger.info("=== Step 5: Interpreting GTM signals ===")
    interpretations = interpret_batch(companies)
    logger.info("Generated %d interpretations.", len(interpretations))
    return interpretations


def run_crm_sync(companies: list[dict], interpretations: list[dict]) -> None:
    logger.info("=== Step 6: Syncing to HubSpot CRM ===")
    process_batch(companies, interpretations)
    logger.info("CRM sync complete.")


def main():
    test_connections()

    logger.info("Starting GTM Intelligence Pipeline")

    try:
        usaspending_records, sec_records, bls_records = run_ingestion()
    except Exception as exc:
        logger.error("Ingestion failed: %s", exc)
        sys.exit(1)

    try:
        run_snowflake_load(usaspending_records, sec_records, bls_records)
    except Exception as exc:
        logger.error("Snowflake load failed: %s", exc)
        sys.exit(1)

    try:
        resolved_companies = run_company_resolution(sec_records)
        enriched_companies = run_enrichment(resolved_companies)
        interpretations = run_signal_interpretation(enriched_companies)
        run_crm_sync(enriched_companies, interpretations)
    except Exception as exc:
        logger.error("Downstream pipeline step failed: %s", exc)
        sys.exit(1)

    logger.info("GTM Intelligence Pipeline completed successfully.")


if __name__ == "__main__":
    main()
