import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

SERIES_IDS = [
    "JTS510000000000000JOL",
    "JTS540000000000000JOL",
    "JTS520000000000000JOL",
    "JTS000000000000000JOL",
]

INDUSTRY_MAP = {
    "JTS510000000000000JOL": "Information Technology",
    "JTS540000000000000JOL": "Professional and Business Services",
    "JTS520000000000000JOL": "Finance and Insurance",
    "JTS000000000000000JOL": "Total Nonfarm",
}

REQUEST_BODY = {
    "seriesid": SERIES_IDS,
    "startyear": "2024",
    "endyear": "2026",
}


def fetch_bls_data() -> list[dict]:
    all_records: list[dict] = []

    try:
        response = requests.post(API_URL, json=REQUEST_BODY, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        logger.error("Error fetching BLS data: %s", exc)
        return all_records

    if data.get("status") != "REQUEST_SUCCEEDED":
        logger.error("BLS API returned non-success status: %s", data.get("status"))
        return all_records

    for series in data.get("Results", {}).get("series", []):
        series_id = series.get("seriesID", "")
        industry = INDUSTRY_MAP.get(series_id, "Unknown")

        for point in series.get("data", []):
            try:
                value = int(point.get("value", "0").replace(",", ""))
            except (ValueError, TypeError):
                value = 0

            record = {
                "series_id": series_id,
                "industry": industry,
                "year": point.get("year"),
                "period": point.get("period"),
                "period_name": point.get("periodName"),
                "value": value,
            }
            all_records.append(record)

    return all_records


def main():
    print("Starting BLS jobs ingestion...")
    records = fetch_bls_data()
    print(f"Total BLS data points fetched: {len(records)}")
    return records


if __name__ == "__main__":
    main()
