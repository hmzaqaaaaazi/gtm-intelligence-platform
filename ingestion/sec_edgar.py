import json
import logging
import time

import requests
from dateutil.parser import parse as parse_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

API_URL = "https://efts.sec.gov/LATEST/search-index"
HEADERS = {"User-Agent": "hamza.q@northeastern.edu"}

PAGE_SIZE = 100
MAX_RECORDS_PER_QUERY = 10000

QUERIES = [
    '"artificial intelligence" OR "machine learning" OR "generative AI"',
    '"SaaS" OR "cloud computing" OR "enterprise software"',
    '"fintech" OR "cybersecurity" OR "data analytics"',
    '"revenue operations" OR "go-to-market" OR "sales automation"',
    '"digital transformation" OR "API" OR "developer tools"',
    '"data analytics" OR "business intelligence" OR "workflow automation"',
    '"HubSpot" OR "Salesforce" OR "CRM" OR "sales enablement"',
    '"outbound" OR "pipeline generation" OR "demand generation" OR "lead generation"',
]

DATE_FILTERS = {
    "forms": "8-K",
    "dateRange": "custom",
    "startdt": "2024-01-01",
    "enddt": "2026-05-03",
}


def parse_hit(hit: dict) -> dict:
    source = hit.get("_source", {})
    raw_adsh = hit.get("_id") or source.get("adsh") or None
    adsh = raw_adsh.split(":")[0] if raw_adsh else None

    display_names = source.get("display_names", [])
    display_name_raw = display_names[0] if display_names else ""

    if "(" in display_name_raw and ")" in display_name_raw:
        company_name = display_name_raw[:display_name_raw.index("(")].strip()
        ticker = display_name_raw[display_name_raw.index("(") + 1:display_name_raw.index(")")].strip()
    else:
        company_name = display_name_raw.strip()
        ticker = None

    cik_parts = display_name_raw.split("CIK")
    cik = cik_parts[1].strip().lstrip(":").strip().split()[0].rstrip(")") if len(cik_parts) > 1 else None

    file_date_raw = source.get("file_date")
    try:
        file_date = parse_date(file_date_raw).date() if file_date_raw else None
    except Exception:
        file_date = None

    biz_locations = source.get("biz_locations", [])
    biz_states = source.get("biz_states", [])
    sics = source.get("sics", [])
    items = source.get("items", [])

    return {
        "adsh": adsh,
        "display_name_raw": display_name_raw,
        "company_name": company_name,
        "ticker": ticker,
        "cik": cik,
        "file_date": file_date,
        "biz_location": biz_locations[0] if biz_locations else None,
        "state_code": biz_states[0] if biz_states else None,
        "sic_code": sics[0] if sics else None,
        "items": json.dumps(items),
    }


def fetch_query(query: str, max_records: int = MAX_RECORDS_PER_QUERY) -> list[dict]:
    """Fetch up to max_records filings for a single query string."""
    records: list[dict] = []
    offset = 0

    while len(records) < max_records:
        params = {**DATE_FILTERS, "q": query, "from": offset, "size": PAGE_SIZE}
        data = None
        for attempt in range(3):
            try:
                response = requests.get(API_URL, params=params, headers=HEADERS, timeout=60)
                response.raise_for_status()
                data = response.json()
                break
            except Exception as exc:
                logger.warning("Attempt %d failed at offset %d for '%s': %s", attempt + 1, offset, query[:40], exc)
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        if data is None:
            logger.error("All retries failed at offset %d for query '%s', stopping.", offset, query[:40])
            break

        hits_wrapper = data.get("hits", {})
        hits = hits_wrapper.get("hits", [])
        if not hits:
            break

        for hit in hits:
            records.append(parse_hit(hit))

        total = hits_wrapper.get("total", {}).get("value", 0)
        offset += len(hits)

        if len(records) % 100 == 0:
            print(f"  [{query[:35]}...] {len(records)} records so far")

        if offset >= total or len(records) >= max_records:
            if len(records) >= max_records:
                logger.info("Reached cap of %d for query: %s", max_records, query[:50])
            break

    return records


def fetch_all_filings(max_records: int = MAX_RECORDS_PER_QUERY) -> list[dict]:
    """
    Run all three queries, combine results, and deduplicate on (cik, file_date).
    """
    combined: list[dict] = []
    seen: set[tuple] = set()

    for i, query in enumerate(QUERIES, 1):
        print(f"\n--- Query {i}/3: {query[:60]} ---")
        results = fetch_query(query, max_records=max_records)
        print(f"  Query {i} returned: {len(results):,} records")

        before = len(combined)
        for record in results:
            key = record.get("adsh")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            combined.append(record)

        added = len(combined) - before
        print(f"  New unique adsh values added: {added:,} (duplicates skipped: {len(results) - added:,})")

    return combined


def main():
    print("Starting SEC EDGAR multi-query ingestion...")
    records = fetch_all_filings()
    print(f"\nTotal deduplicated SEC filings: {len(records):,}")
    return records


if __name__ == "__main__":
    main()
