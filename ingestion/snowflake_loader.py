import logging
import os

import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.getenv("SNOWFLAKE_DATABASE", "GTM_INTELLIGENCE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def load_usaspending(records: list[dict]) -> None:
    if not records:
        logger.warning("No USASpending records to load.")
        return

    table = "GTM_INTELLIGENCE.RAW.usaspending_awards"
    insert_sql = f"""
        INSERT INTO {table}
            (recipient_name, award_amount, awarding_agency, state_code,
             award_id, start_date, naics_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            r["recipient_name"], r["award_amount"], r["awarding_agency"],
            r["state_code"], r["award_id"], r["start_date"], r["naics_code"],
        )
        for r in records
    ]

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")
            cur.executemany(insert_sql, rows)
        conn.commit()
        print(f"Rows inserted into {table}: {len(rows)}")
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to load USASpending data: %s", exc)
        raise
    finally:
        conn.close()


def load_sec_filings(records: list[dict]) -> None:
    if not records:
        logger.warning("No SEC filing records to load.")
        return

    table = "GTM_INTELLIGENCE.RAW.sec_8k_filings"
    insert_sql = f"""
        INSERT INTO {table}
            (adsh, display_name_raw, company_name, ticker, cik,
             file_date, biz_location, state_code, sic_code, items)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            r["adsh"], r["display_name_raw"], r["company_name"], r["ticker"], r["cik"],
            r["file_date"], r["biz_location"], r["state_code"], r["sic_code"], r["items"],
        )
        for r in records
    ]

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")
            cur.executemany(insert_sql, rows)
        conn.commit()
        print(f"Rows inserted into {table}: {len(rows)}")
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to load SEC filings data: %s", exc)
        raise
    finally:
        conn.close()


def load_bls_jobs(records: list[dict]) -> None:
    if not records:
        logger.warning("No BLS job records to load.")
        return

    table = "GTM_INTELLIGENCE.RAW.bls_job_openings"
    insert_sql = f"""
        INSERT INTO {table}
            (series_id, industry, year, period, period_name, value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            r["series_id"], r["industry"], r["year"],
            r["period"], r["period_name"], r["value"],
        )
        for r in records
    ]

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")
            cur.executemany(insert_sql, rows)
        conn.commit()
        print(f"Rows inserted into {table}: {len(rows)}")
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to load BLS jobs data: %s", exc)
        raise
    finally:
        conn.close()
