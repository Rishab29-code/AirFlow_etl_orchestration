# Apache Airflow ETL Pipeline with Dashboard

## Project Overview

This project is an end-to-end ETL pipeline built using Apache Airflow, Docker, PostgreSQL, and Streamlit.

The pipeline extracts product data from an API, transforms it, validates data quality, stores it in PostgreSQL, generates analytics tables, and displays results through a dashboard.

---

## Architecture

```text
DummyJSON API
      ↓
Extract
      ↓
Transform
      ↓
Data Quality Checks
      ↓
PostgreSQL Warehouse
      ↓
Analytics Table
      ↓
Streamlit Dashboard
```

---

## Features

- Extract product data from API
- Transform and clean data using Pandas
- Data quality validation
- Incremental loading using PostgreSQL UPSERT
- Analytics table generation
- Airflow orchestration
- Logging for monitoring
- Streamlit dashboard with KPIs and charts
- Dockerized local setup

---

## Tech Stack

- Python
- Apache Airflow
- Docker
- PostgreSQL
- Pandas
- Requests
- Streamlit

---

## Project Structure

```text
airflow-etl-project/
│
├── dags/
│   └── etl_pipeline.py
│
├── dashboard.py
│
├── screenshots/
│   ├── airflow_dag.png
│   └── dashboard.png
│
├── docker-compose.yaml
│
├── requirements.txt
│
├── README.md
│
└── .gitignore
```

---

## Pipeline Flow

1. Extract product data from API
2. Transform required columns and business logic
3. Run data quality checks
4. Load data into PostgreSQL warehouse
5. Generate analytics summary table
6. Display KPIs and charts in Streamlit dashboard

---

## Dashboard Preview

Add screenshots here.

![Dashboard](screenshots/dashboard.png)

![Airflow DAG](screenshots/airflow_dag.png)

---

## How to Run

Clone repository:

```bash
git clone <your-repository-url>
cd airflow-etl-project
```

Start Docker services:

```bash
docker compose up -d
```

Open Airflow:

```text
http://localhost:8081
```

Run dashboard:

```bash
streamlit run dashboard.py
```

---

## Future Improvements

- Add alerting and notifications
- Add cloud deployment
- Add Kafka for streaming ingestion
- Add advanced reporting