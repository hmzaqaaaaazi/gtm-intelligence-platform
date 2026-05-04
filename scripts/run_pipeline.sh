#!/bin/bash
echo "Starting GTM Intelligence Pipeline"
echo "Step 1: Running ingestion"
python -m ingestion.usaspending
python -m ingestion.sec_edgar
python -m ingestion.bls_jobs
echo "Step 2: Loading to Snowflake"
python -m orchestration.pipeline
echo "Pipeline complete"
