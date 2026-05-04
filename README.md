# GTM Intelligence Platform

An automated GTM data pipeline that ingests federal government signals, scores companies by buying intent, enriches contacts, and pushes high-intent accounts into HubSpot CRM with AI-generated intelligence notes.

## What This Does

Pulls three federal data sources on a biweekly schedule, transforms and scores 4,000+ companies by buying intent, enriches top accounts with contact data, and routes them into HubSpot with full context for the sales team. A compliance monitor then tracks pipeline health daily.

## Architecture
Federal APIs (USASpending + SEC EDGAR + BLS JOLTS)
↓
Snowflake RAW Schema (ingestion layer)
↓
dbt Models (staging → intermediate → marts)
↓
mart_intent_scores (4,014 unique companies scored 0-100)
↓
Company Resolution Agent (Groq LLM — canonical names + domains)
↓
Hunter.io Enrichment (contact emails)
↓
Signal Interpretation Agent (Groq LLM — talking points + urgency)
↓
HubSpot CRM (513 companies pushed with intelligence notes)
↓
GitHub Actions Compliance Monitor (daily pipeline health checks)

## Data Sources

| Source | Records | Signal Type |
|---|---|---|
| USASpending.gov | 12,572 federal contracts | Contract award signals |
| SEC EDGAR | 21,545 8-K filings | Company event signals |
| BLS JOLTS | 78 data points | Sector hiring trends |

## Intent Scoring Model

| Component | Max Points | Logic |
|---|---|---|
| Federal Contract Award | 40 | Award size and recency |
| SEC Filing Recency | 30 | Days since last 8-K |
| Sector Hiring Demand | 30 | BLS job openings vs average |
| SaaS/Software Boost | +10 | SIC code 7370-7379 |

## Tier Classification

| Tier | Score Range | Action |
|---|---|---|
| High | 80-100 | Immediate outreach |
| Medium | 60-79 | Automated sequence |
| Low | Below 60 | Monitor |

## Results

- 4,014 unique companies scored across High, Medium, and Low intent tiers
- 513 companies pushed to HubSpot CRM with AI intelligence notes
- Contact enrichment via Hunter.io API for verified company domains
- 9 Immediate Outreach targets identified by signal interpretation agent

## How to Run

Install dependencies:
pip install -r requirements.txt

Set environment variables — copy .env.example to .env and fill in credentials.

Run full pipeline:
python -m orchestration.pipeline

Run individual steps:
python -m ingestion.usaspending
python -m ingestion.sec_edgar
python -m ingestion.bls_jobs
cd dbt && dbt run --profiles-dir .

Run compliance checks:
python -m compliance.mql_compliance
python -m compliance.stale_deals
python -m compliance.pipeline_delta

## Project Structure
gtm-intelligence-platform/
├── ingestion/          # Federal API ingestion scripts
├── dbt/                # dbt models: staging → intermediate → marts
├── agents/             # LLM-powered resolution and interpretation
├── enrichment/         # Contact enrichment
├── compliance/         # HubSpot pipeline health monitoring
├── orchestration/      # End-to-end pipeline runner
└── .github/workflows/  # Scheduled GitHub Actions

## dbt Models

| Layer | Model | Description |
|---|---|---|
| Staging | stg_usaspending | Clean federal award records |
| Staging | stg_sec_filings | Parsed 8-K filings |
| Staging | stg_bls_jobs | Job opening time series |
| Intermediate | int_company_signals | Joined company signals |
| Mart | mart_intent_scores | Final intent scores 0-100 with tier |

## Compliance Monitoring

Three automated GitHub Actions workflows run on schedule:

| Workflow | Schedule | What It Does |
|---|---|---|
| MQL Compliance | Weekdays 9am UTC | Flags MQLs with no activity in 24 hours |
| Stale Deal Alert | Daily 8am UTC | Flags open deals with no activity in 14 days |
| Pipeline Delta | Mondays 7am UTC | Compares pipeline to prior week, synthesizes AI narrative, posts to Slack for approval |

Pipeline snapshot stored in compliance/pipeline_snapshot.json and committed to git after each run for full audit trail.

## Environment Variables

| Variable | Description |
|---|---|
| SNOWFLAKE_ACCOUNT | Snowflake account identifier |
| SNOWFLAKE_USER | Snowflake username |
| SNOWFLAKE_PASSWORD | Snowflake password |
| SNOWFLAKE_DATABASE | Target database |
| SNOWFLAKE_WAREHOUSE | Compute warehouse |
| SNOWFLAKE_ROLE | Role |
| GROQ_API_KEY | Groq API key for LLM agents |
| HUNTER_API_KEY | Hunter.io API key |
| HUBSPOT_ACCESS_TOKEN | HubSpot access token |
| SLACK_WEBHOOK_URL | Slack webhook for alerts |
| SEC_USER_AGENT | Email for SEC EDGAR User-Agent header |

## Tech Stack

Python, Snowflake, dbt, Docker, Prefect, LangGraph, Groq API, Hunter.io, HubSpot API, GitHub Actions, USASpending API, SEC EDGAR API, BLS API
