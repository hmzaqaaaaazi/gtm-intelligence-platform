"""
USASpending ingestion via Awards Search API.
Paginates using hasNext from page_metadata. Accepts arbitrary date ranges.
"""
import logging
import time

import requests
from dateutil.parser import parse as parse_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SEARCH_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
PAGE_SIZE = 100
MAX_RECORDS = 5000

NAICS_CODES = [
    "541511", "541512", "541519",
    "541611", "541612", "541613", "541614", "541618", "541690",
    "518210", "519130",
    "517311", "517312",
    "561320", "611420",
    "541330", "541714", "541715",
]

FIELDS = [
    "Award ID",
    "Recipient Name",
    "Award Amount",
    "Awarding Agency",
    "Place of Performance State Code",
    "Start Date",
    "NAICS Code",
]


def parse_row(row: dict) -> dict:
    raw_amount = row.get("Award Amount") or 0
    try:
        award_amount = float(str(raw_amount).replace(",", "").strip() or 0)
    except (ValueError, TypeError):
        award_amount = 0.0

    raw_date = (row.get("Start Date") or "").strip()
    try:
        start_date = parse_date(raw_date).date() if raw_date else None
    except Exception:
        start_date = None

    state_code = (row.get("Place of Performance State Code") or "").strip()[:2] or None

    return {
        "award_id":        (row.get("Award ID") or "").strip() or None,
        "recipient_name":  (row.get("Recipient Name") or "").strip() or None,
        "award_amount":    award_amount,
        "awarding_agency": (row.get("Awarding Agency") or "").strip() or None,
        "state_code":      state_code,
        "start_date":      start_date,
        "naics_code":      str(row.get("NAICS Code") or "").strip() or None,
    }


def fetch_date_range(start_date: str, end_date: str, max_records: int = MAX_RECORDS) -> list[dict]:
    """Fetch awards for a single date range, paginating until hasNext=False or max_records hit."""
    label = f"{start_date}→{end_date}"
    records = []
    page = 1

    while len(records) < max_records:
        payload = {
            "filters": {
                "award_type_codes": ["A", "B", "C", "D"],
                "time_period": [{"start_date": start_date, "end_date": end_date}],
                "naics_codes": NAICS_CODES,
            },
            "fields": FIELDS,
            "page": page,
            "limit": PAGE_SIZE,
            "sort": "Award Amount",
            "order": "desc",
        }
        for attempt in range(3):
            try:
                resp = requests.post(SEARCH_URL, json=payload, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as exc:
                logger.warning("[%s] page %d attempt %d failed: %s", label, page, attempt + 1, exc)
                if attempt < 2:
                    time.sleep(5)
                else:
                    data = None

        if data is None:
            logger.error("[%s] All retries failed on page %d, stopping.", label, page)
            break

        results = data.get("results", [])
        if not results:
            break

        for row in results:
            records.append(parse_row(row))

        has_next = data.get("page_metadata", {}).get("hasNext", False)
        print(f"  [{label}] page {page} → {len(records):,} records total | hasNext={has_next}")

        if not has_next or len(records) >= max_records:
            break

        page += 1
        time.sleep(0.3)

    logger.info("[%s] Fetched %d records.", label, len(records))
    return records


def fetch_all_awards(date_ranges: list[tuple[str, str]] = None) -> list[dict]:
    """
    Fetch awards across multiple date ranges, deduplicate on award_id.
    date_ranges: list of (start_date, end_date) tuples.
    """
    if date_ranges is None:
        date_ranges = [
            ("2024-01-01", "2024-06-30"),
            ("2024-07-01", "2024-12-31"),
            ("2025-01-01", "2025-06-30"),
            ("2025-07-01", "2025-12-31"),
            ("2026-01-01", "2026-03-31"),
            ("2026-04-01", "2026-05-03"),
        ]

    all_records: list[dict] = []
    seen_ids: set[str] = set()

    for start, end in date_ranges:
        print(f"\n--- Date range: {start} → {end} ---")
        records = fetch_date_range(start, end)
        print(f"  Raw: {len(records):,}")
        added = 0
        for r in records:
            aid = r.get("award_id") or ""
            if aid and aid in seen_ids:
                continue
            if aid:
                seen_ids.add(aid)
            all_records.append(r)
            added += 1
        print(f"  Unique added: {added:,} | running total: {len(all_records):,}")

    print(f"\n=== USASpending total (deduplicated): {len(all_records):,} ===")
    return all_records


def main():
    print("Starting USASpending ingestion...")
    records = fetch_all_awards()
    print(f"\nSample: {records[0] if records else 'none'}")
    return records


if __name__ == "__main__":
    main()
