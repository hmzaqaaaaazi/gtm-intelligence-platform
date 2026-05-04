"""
Creates the GTM_INTELLIGENCE database, RAW schema, and all raw tables in Snowflake.
Run once before the ingestion pipeline.
"""
import os
import sys
import logging

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "CREATE DATABASE IF NOT EXISTS GTM_INTELLIGENCE",
    "CREATE SCHEMA IF NOT EXISTS GTM_INTELLIGENCE.RAW",
    """
    CREATE TABLE IF NOT EXISTS GTM_INTELLIGENCE.RAW.usaspending_awards (
        id                INTEGER AUTOINCREMENT PRIMARY KEY,
        recipient_name    VARCHAR(500),
        award_amount      FLOAT,
        awarding_agency   VARCHAR(500),
        state_code        VARCHAR(10),
        award_id          VARCHAR(200),
        start_date        DATE,
        naics_code        VARCHAR(20),
        inserted_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS GTM_INTELLIGENCE.RAW.sec_8k_filings (
        id               INTEGER AUTOINCREMENT PRIMARY KEY,
        adsh             VARCHAR(25),
        display_name_raw VARCHAR(1000),
        company_name     VARCHAR(500),
        ticker           VARCHAR(20),
        cik              VARCHAR(20),
        file_date        DATE,
        biz_location     VARCHAR(500),
        state_code       VARCHAR(10),
        sic_code         VARCHAR(10),
        items            VARCHAR(2000),
        inserted_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS GTM_INTELLIGENCE.RAW.bls_job_openings (
        id          INTEGER AUTOINCREMENT PRIMARY KEY,
        series_id   VARCHAR(50),
        industry    VARCHAR(200),
        year        INTEGER,
        period      VARCHAR(10),
        period_name VARCHAR(50),
        value       FLOAT,
        inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS GTM_INTELLIGENCE.RAW.enriched_contacts (
        id              INTEGER AUTOINCREMENT PRIMARY KEY,
        company_name    VARCHAR(500),
        domain          VARCHAR(200),
        first_name      VARCHAR(100),
        last_name       VARCHAR(100),
        email           VARCHAR(200),
        position        VARCHAR(200),
        confidence      INTEGER,
        inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS GTM_INTELLIGENCE.RAW.gtm_signals (
        id              INTEGER AUTOINCREMENT PRIMARY KEY,
        company_name    VARCHAR(500),
        signal_type     VARCHAR(100),
        signal_summary  VARCHAR(4000),
        intent_score    INTEGER,
        recommended_action VARCHAR(2000),
        source          VARCHAR(100),
        inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """,
]


def setup():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

    try:
        with conn.cursor() as cur:
            for stmt in DDL:
                stmt = stmt.strip()
                label = stmt.split("\n")[0][:80]
                print(f"  Running: {label}...")
                cur.execute(stmt)
                print(f"    OK")
        conn.commit()
        print("\nSnowflake setup complete. All tables ready.")
    except Exception as exc:
        logger.error("Setup failed: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    setup()
