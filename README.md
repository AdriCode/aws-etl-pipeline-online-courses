# AWS End-to-End Data Engineering Pipeline

## Overview
An end-to-end cloud data engineering pipeline built on AWS that ingests 
216,000+ online course records from Udemy and Coursera, performs ETL 
transformation, stores data in a star schema, and delivers insights 
through OLAP queries and a Tableau dashboard.

## Architecture
Raw CSV (S3) → Glue Crawler → Glue ETL (PySpark) → Processed Parquet (S3) → Athena (OLAP SQL) → Tableau

## Tech Stack
- **Storage:** Amazon S3 (two-zone data lake)
- **Cataloging:** AWS Glue Crawler + Data Catalog
- **Transformation:** AWS Glue ETL (PySpark)
- **Querying:** Amazon Athena (OLAP SQL)
- **Visualisation:** Tableau
- **File Format:** Parquet (columnar)
- **Schema Design:** Star Schema (1 fact, 4 dimensions)

## Datasets
- Udemy: 209,734 courses (hossaingh/udemy-courses)
- Coursera: 6,645 courses (azraimohamad/coursera-courses-2024)
- Total after union: 216,379 records

## Pipeline Phases
1. **Ingestion** — Raw CSVs uploaded to S3 raw bucket
2. **Cataloging** — Glue Crawlers auto-detect schema into Data Catalog
3. **Transformation** — Glue ETL cleans, unifies, builds star schema
4. **Storage** — Parquet files written to S3 processed bucket
5. **Analytics** — OLAP queries via Athena (ROLLUP, CUBE, RANK, NTILE)
6. **Visualisation** — Tableau dashboard with 6 interactive visuals

## Key Transformations
- Unified mismatched schemas from two platforms
- Custom UDF to parse Coursera enrollment strings ("10k-50k" → numeric)
- Feature engineering: price_category, published_year, published_month
- Star schema: fact_courses + dim_platform, dim_category, dim_certificate, dim_date

## OLAP Operations
- ROLLUP: enrollment aggregation from subject → platform → grand total
- CUBE: all combinations of platform, level, certificate type
- RANK(): subject ranking by enrollment within each platform
- NTILE(4): enrollment quartile distribution analysis

## Results
- 216,379 courses unified across Udemy and Coursera
- Technology and Business subjects dominate enrollment on both platforms
- Coursera maintains higher average ratings despite lower course volume
- Long-tail distribution confirmed — top 25% courses drive majority of enrollments

## Screenshots
See /screenshots folder
