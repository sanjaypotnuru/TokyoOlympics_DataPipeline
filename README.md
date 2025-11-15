# TokyoOlympics Data Pipeline

A small data pipeline that ingests, stores, transforms and publishes Tokyo Olympics medal data using a medallion architecture (Bronze, Silver, Gold).

## Key idea
- Data is ingested from source(s) and landed raw (Bronze).
- Data is cleaned/enriched and organized (Silver).
- Aggregated, analytics-ready datasets are produced for reporting (Gold).

## Architecture (high level)
- Ingestion: Data Factory / custom ingest scripts
- Raw store: Data Lake Gen2 (Bronze)
- Transformation: Databricks / Spark jobs (Silver)
- Serving / Analytics: Azure Synapse / curated Gold tables
- Visualization: Power BI / Looker Studio / Tableau

## Medal (medallion) layers
- Bronze (raw)
  - Exact copy of ingested files/tables. Minimal processing.
- Silver (cleaned / standardized)
  - Type fixes, deduplication, normalized fields, joins across sources.
- Gold (business / reporting)
  - Aggregations, star-schema tables and materialized views used by dashboards.

## Quickstart
1. Clone repository
   - git clone https://github.com/sanjaypotnuru/TokyoOlympics_DataPipeline
   - cd TokyoOlympics_Data_Pipeline
2. Configure
   - Copy .env.example to .env and set cloud/storage credentials and endpoints.
3. Run
   - Ingest raw data: python scripts/ingest.py  (or run your Data Factory pipeline)
   - Transform to Silver/Gold: python scripts/transform.py --stage silver --stage gold
   - Serve / load to analytics: python scripts/load_analytics.py
   - Or run orchestration tool / notebooks used in this project.

## Project layout (example)
- scripts/         - ingestion and transformation scripts
- notebooks/       - exploration and ETL notebooks
- configs/         - pipeline configuration and .env examples
- infrastructure/  - IaC templates (ARM/Bicep/Terraform)
- docs/            - architecture diagrams and runbooks

## Configuration
- Required environment variables: STORAGE_ACCOUNT, CONTAINER, SPARK_MASTER, etc.
- Add credentials to .env (never commit secrets).

## Tests
- Unit tests (if available): pytest
- Integration tests: run against dev environment with test credentials.

## Contribution
- Fork, create branch, add tests, open PR with description of changes.

## License
- Add a LICENSE file (e.g., MIT) and