# GTM Intelligence Platform

A GTM data pipeline that ingests federal government data into Snowflake and enriches it with AI-powered company signals for sales and marketing teams.

## Overview

This platform pulls data from three federal sources, loads it into Snowflake, transforms it with dbt, and surfaces intent scores for outbound GTM motions.

### Data Sources
- **USASpending.gov** — Federal contract awards in IT/consulting NAICS codes
- **SEC EDGAR** — 8-K filings from software, SaaS, and AI companies
- **BLS** — Job openings by sector (Information Technology, Finance, Professional Services)

### Downstream Actions
- AI-powered company resolution (Groq LLaMA3)
- GTM signal interpretation with talking points
- HubSpot CRM enrichment and note creation
- Hunter.io contact discovery

## Project Structure

```
gtm-intelligence-platform/
├── ingestion/          # Data ingestion from federal APIs
├── dbt/                # dbt models: staging → intermediate → marts
├── agents/             # LLM-powered resolution and interpretation
├── enrichment/         # Hunter.io contact enrichment
├── compliance/         # HubSpot MQL/deal compliance checks
├── orchestration/      # End-to-end pipeline runner
└── .github/workflows/  # Scheduled GitHub Actions
```

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

3. Set up Snowflake tables:
   - Database: `GTM_INTELLIGENCE`
   - Schema: `RAW`
   - Tables: `usaspending_awards`, `sec_8k_filings`, `bls_job_openings`

4. Configure dbt profile (`~/.dbt/profiles.yml`) for the `gtm_intelligence` profile targeting Snowflake.

## Running the Pipeline

### Full pipeline
```bash
python -m orchestration.pipeline
```

### Individual ingestion scripts
```bash
python -m ingestion.usaspending
python -m ingestion.sec_edgar
python -m ingestion.bls_jobs
```

### Compliance checks
```bash
python -m compliance.mql_compliance
python -m compliance.stale_deals
python -m compliance.pipeline_delta
```

### Docker
```bash
docker-compose up pipeline
```

## dbt Models

| Layer | Model | Description |
|-------|-------|-------------|
| Staging | `stg_usaspending` | Clean federal award records |
| Staging | `stg_sec_filings` | Parsed 8-K filings |
| Staging | `stg_bls_jobs` | Job opening time series |
| Intermediate | `int_company_signals` | Joined company signals |
| Mart | `mart_intent_scores` | Final intent scores (0–100) with tier |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_DATABASE` | Target database (default: `GTM_INTELLIGENCE`) |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse (default: `COMPUTE_WH`) |
| `SNOWFLAKE_ROLE` | Role (default: `ACCOUNTADMIN`) |
| `GROQ_API_KEY` | Groq API key for LLM agents |
| `HUNTER_API_KEY` | Hunter.io API key for contact enrichment |
| `HUBSPOT_ACCESS_TOKEN` | HubSpot private app access token |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook for alerts |
| `SEC_USER_AGENT` | Email for SEC EDGAR User-Agent header |
| `SLACK_LEADERSHIP_WEBHOOK_URL` | Slack webhook for leadership channel (pipeline delta publish) |

## Results

### Pipeline Run — May 2026

- **513** High + Medium tier companies queried from Snowflake mart
- **513** companies resolved via Groq `llama-3.1-8b-instant` (8s, 10 threads)
- **231** contacts enriched via Hunter.io across 26 companies with verified domains
- **9** Immediate Outreach · **1** Nurture · **503** Monitor (signal interpretation)
- **513** companies upserted to HubSpot CRM (0 failures, 57s)

## Compliance Monitoring

Three automated GitHub Actions workflows run on schedule:

| Workflow | Schedule | What It Does |
|---|---|---|
| MQL Compliance | Weekdays 9am UTC | Flags MQLs with no activity in 24 hours |
| Stale Deal Alert | Daily 8am UTC | Flags open deals with no activity in 14 days |
| Pipeline Delta | Mondays 7am UTC | Compares pipeline to prior week, synthesizes AI narrative, sends to Slack for approval |

Pipeline snapshot stored in `compliance/pipeline_snapshot.json` and committed to git after each run for full audit trail.

## Deals Created

9 high-intent accounts converted to HubSpot deals:
- Total pipeline ACV seeded: $20,408,588
- Deal stage: Appointment Scheduled
- ACV estimated at 0.1% of federal contract award value

| Company | Intent Score | Est. ACV |
|---|---|---|
| Science Applications International Corporation | 100/100 | $18,133,067 |
| Lockheed Martin Corporation | 100/100 | $1,256,170 |
| Vir Biotechnology, Inc. | 90/100 | $658,678 |
| Axon Enterprise, Inc. | 75/100 | $192,638 |
| Tempus AI, Inc. | 90/100 | $125,546 |
| Amgen Inc. | 100/100 | $26,409 |
| CRA International, Inc. | 90/100 | $16,080 |
| Matthews International Corporation | 60/100 | $0 |
| XMax Inc. | 60/100 | $0 |
