# 💼 Data Engineer Job Postings ETL (LinkedIn API → PostgreSQL)

This ETL pipeline extracts real-time job postings for Data Engineer roles from the LinkedIn API (or similar job data source), processes key job attributes, and loads the results into a PostgreSQL database. It's built to support job market analysis, personal job tracking, and insights into hiring trends across Canada and the U.S.

## 🎯 Objectives

- Automate retrieval of Data Engineer job listings
- Standardize fields such as title, company, location, date posted, and seniority
- Store into a structured PostgreSQL database for querying, reporting, or alerting
- Enable downstream analysis using BI tools like Power BI or Metabase

## 🧰 Stack Overview

| Component        | Purpose                        |
|------------------|--------------------------------|
| Python           | Core ETL logic                 |
| Requests         | API interaction                |
| Pandas           | Data wrangling & cleaning      |
| SQLAlchemy       | DB connectivity                |
| PostgreSQL       | Data warehouse                 |
| Apache Airflow   | DAG orchestration & scheduling |
| dotenv           | API key/secret management      |

## 🔄 Pipeline Flow
