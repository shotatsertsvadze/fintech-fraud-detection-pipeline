# FinTech Fraud Detection Pipeline

A data engineering portfolio project that simulates a fraud detection pipeline for transaction data using Python and a layered architecture (Raw → Silver → Gold).

---

## Demo

Run the full pipeline:

```bash
python -m src.pipeline.run_pipeline
```

---

## Overview

This project models a simplified fraud detection system used in FinTech environments.

The pipeline:
1. Ingests raw transaction data (CSV)
2. Cleans and validates the data (Silver layer)
3. Applies rule-based fraud detection and scoring
4. Generates analytics for monitoring fraud
5. Aggregates business metrics (Gold layer)

---

## Architecture

```text
[RAW CSV]
   ↓
[Silver Layer]
- Data cleaning
- Schema validation
- Status normalization
- Fraud scoring
   ↓
[Analytics]
- Fraud rate
- Revenue breakdown
- Risk analysis
   ↓
[Gold Layer]
- Daily aggregated fraud metrics
```

---

## Project Structure

```text
data/
  raw/        # raw transaction data
  silver/     # cleaned and enriched data
  gold/       # aggregated outputs

src/
  silver/
    clean_transactions.py
  analytics/
    run_analytics.py
  gold/
    build_daily_summary.py
  pipeline/
    run_pipeline.py
  utils/
    logger.py

sql/
  analytics_queries.sql
```

---

## How to Run

From the project root directory:

```bash
python -m src.pipeline.run_pipeline
```

This will:
- clean raw data
- apply fraud logic
- generate analytics
- build gold summary

---

## Example Output

### Silver Layer

- Input rows: 11
- Output rows: 8

Rows removed:
- invalid amounts
- duplicates
- malformed records

Additional columns created:
- `fraud_score`
- `fraud_flag`
- `risk_level`
- `high_amount_flag`
- `many_transactions_flag`
- `suspicious_country_flag`

---

### Analytics

```text
Total transactions: 8
Total revenue: 15991.45
Fraud transactions: 2
Fraud rate: 25%
```

Revenue by country:

```text
DE 9471.25
FR 6400.00
IT   75.20
PL   45.00
```

High risk transactions:

```text
None detected
```

---

### Gold Layer (Daily Summary)

```text
transaction_date | total_transactions | total_revenue | fraud_transactions | fraud_revenue | fraud_rate
2025-01-01       | 8                  | 15991.45      | 2                  | 14300.00      | 0.25
```

---

## Fraud Detection Logic

Fraud score is calculated using rule-based signals:

- High transaction amount (> 5000) → +50
- Multiple transactions per user → +30
- Suspicious country (NG, RU) → +40

Risk classification:
- `low` (< 50)
- `medium` (50–79)
- `high` (80+)

---

## Data Quality Checks

The pipeline enforces:

- Required column validation
- Null value filtering
- Positive transaction amounts only
- Valid country codes (2-letter ISO format)
- Status normalization:
  - approved
  - declined
  - pending
- Duplicate transaction removal

---

## Tech Stack

- Python (pandas)
- SQL (analytics queries)
- CSV-based data lake structure
- Logging for pipeline monitoring

---

## Key Features

- Layered data architecture (Raw → Silver → Gold)
- Reproducible pipeline execution
- Rule-based fraud scoring system
- Data validation and cleaning
- Aggregated business reporting
- Modular and extensible project structure

---

## Next Improvements

Planned upgrades:

- Config-driven pipeline (YAML)
- Replace CSV with Parquet + partitioning
- Add orchestration (Airflow / Prefect)
- Integrate with AWS (S3, Athena)
- Simulate real-time streaming (Kinesis-style)

---

## Purpose

This project demonstrates practical data engineering skills:

- data cleaning and validation
- transformation pipelines
- fraud detection logic
- analytics and aggregation
- project structuring and reproducibility