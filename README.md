# MSCI ESG Data Pipeline

This project implements a robust data pipeline for retrieving, transforming, and storing ESG (Environmental, Social, Governance) data from the official MSCI ESG API. It is designed to facilitate the extraction of issuer, fund, and instrument ESG metrics and seamlessly ingest them into a Microsoft SQL Server database for downstream analytics.

## Overview

### Purpose

The application automates the following:
- Authentication with MSCI's secured API.
- Data retrieval from multiple ESG endpoints.
- Concurrent and scalable processing using Python's thread pool.
- Structured transformation of the raw JSON into tabular data.
- Database insertion using a SQLAlchemy-powered ORM layer.

This tool is valuable for financial institutions, data scientists, and ESG analysts aiming to integrate MSCI ESG insights into their analytical workflows.

## Source of Data

This project consumes data from the [MSCI ESG API v3.0](https://www.msci.com/our-solutions/esg-investing), a proprietary data source that delivers:
- ESG ratings for issuers and funds.
- Time series data for instruments.
- Metadata and parameter coverage for deep querying capabilities.

Access to the API requires valid credentials provided by MSCI.

## Application Flow

The entry point is `main.py`, which coordinates the following operations:

1. **Initialize & Authenticate**:
   - `Engine` is instantiated with MSCI credentials.
   - Logs into the MSCI API and fetches an access token.

2. **Fetch Data**:
   - Based on the `APP_ID`, appropriate MSCI endpoints are called:
     - `LIVE`: Fetches latest `issuers`, `funds`, and `coverages`.
     - `INST_HIST`: Fetches historical `instruments` data for ISINs from the DB.

3. **Transform Data**:
   - Raw JSON is cleaned and transformed to tabular format.

4. **Store in DB**:
   - SQLAlchemy handles bulk inserts into the target MSSQL tables.

## Project Structure

```
msci-esg-main/
├── client/              # MSCI API client and engine logic
│   ├── msci.py          # Auth & API interaction
│   └── engine.py        # Execution engine for data fetching
├── config/              # Environment and logging setup
├── database/            # DB connectors and inserters
├── transformer/         # Data transformation logic
├── main.py              # Application entry point
├── .env.sample          # Sample environment configuration
├── Dockerfile           # Docker containerization setup
```

## Environment Variables

You must configure a `.env` file based on `.env.sample`. Key variables include:

| Variable | Description |
|---------|-------------|
| `APP_ID` | Run mode: `LIVE` or `INST_HIST` |
| `CLIENT_ID`, `CLIENT_SECRET` | MSCI API credentials |
| `ISSUER_FIELDS`, `FUND_FIELDS`, `INSTRUMENT_FIELDS` | Comma-separated list of factors to request |
| `ISSUER_TIMEDELTA_DAYS`, `FUND_TIMEDELTA_DAYS`, `INSTRUMENT_TIMEDELTA_DAYS` | Date range for historical data |
| `ISSUER_TABLE`, `FUND_TABLE`, `..._HISTORY_TABLE` | Destination table names |
| `MSSQL_*` | SQL Server connection credentials |
| `INSERTER_MAX_RETRIES`, `REQUEST_MAX_RETRIES`, `REQUEST_BACKOFF_FACTOR` | Retry and backoff settings for resilience |

Use a `.env` file or inject variables through CI/CD pipelines or Docker secrets.

## Docker Support

This project is Dockerized for portability.

### Build the Image
```bash
docker build -t msci-esg .
```

### Run the Container
```bash
docker run --env-file .env msci-esg
```

## Requirements

Install dependencies via pip:
```bash
pip install -r requirements.txt
```

## Running the App

Make sure `.env` is properly configured, then run:

```bash
python main.py
```

Logs will show progress on fetching and loading data.

## License

This project is licensed under the MIT License. Note that usage of MSCI data must comply with their terms of service.
