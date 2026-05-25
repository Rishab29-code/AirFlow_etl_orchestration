from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2
import logging





logger = logging.getLogger(__name__)


def extract():

    logger.info("Starting extract task")

    response = requests.get(
        "https://dummyjson.com/products?limit=100"
    )

    logger.info(f"API status code: {response.status_code}")

    data = response.json()

    df = pd.DataFrame(
        data["products"]
    )

    logger.info(f"Extracted {len(df)} records")

    df.to_csv(
        "/opt/airflow/extracted_data.csv",
        index=False
    )

    logger.info("Extract task completed")


def transform():

    logger.info("Starting transform task")

    df = pd.read_csv(
        "/opt/airflow/extracted_data.csv"
    )

    logger.info(f"Rows before transform: {len(df)}")

    df = df[
        ["id", "title", "price", "rating"]
    ]

    df["price"] = df["price"] * 1.1

    df.to_csv(
        "/opt/airflow/transformed_data.csv",
        index=False
    )

    logger.info(f"Rows after transform: {len(df)}")
    logger.info("Transform task completed")


def data_quality():

    logger.info("Starting data quality checks")

    df = pd.read_csv(
        "/opt/airflow/transformed_data.csv"
    )

    if df["title"].isnull().sum() > 0:
        logger.error("Null titles found")
        raise Exception("Null titles found")

    if (df["price"] < 0).sum() > 0:
        logger.error("Negative prices found")
        raise Exception("Negative prices found")

    if ((df["rating"] < 0) | (df["rating"] > 5)).sum() > 0:
        logger.error("Invalid ratings found")
        raise Exception("Invalid ratings found")

    if df["id"].duplicated().sum() > 0:
        logger.error("Duplicate IDs found")
        raise Exception("Duplicate IDs found")

    logger.info("Data quality checks passed")


def load():

    logger.info("Starting load task")

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products(
        id INT PRIMARY KEY,
        title VARCHAR(255),
        price FLOAT,
        rating FLOAT
    )
    """)

    df = pd.read_csv(
        "/opt/airflow/transformed_data.csv"
    )

    logger.info(f"Loading {len(df)} rows into PostgreSQL")

    insert_query = """
    INSERT INTO products (
        id,
        title,
        price,
        rating
    )
    VALUES (%s, %s, %s, %s)

    ON CONFLICT (id)
    DO UPDATE SET
        title = EXCLUDED.title,
        price = EXCLUDED.price,
        rating = EXCLUDED.rating;
    """

    for _, row in df.iterrows():

        cursor.execute(
            insert_query,
            (
                row["id"],
                row["title"],
                row["price"],
                row["rating"]
            )
        )

    conn.commit()

    logger.info("Load task completed")

    cursor.close()
    conn.close()


def analytics():

    logger.info("Starting analytics task")

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    cursor.execute("""
    DROP TABLE IF EXISTS product_summary
    """)

    cursor.execute("""
    CREATE TABLE product_summary AS
    SELECT
        COUNT(*) AS total_products,
        ROUND(AVG(price)::numeric,2) AS avg_price,
        ROUND(MAX(price)::numeric,2) AS max_price,
        ROUND(MIN(price)::numeric,2) AS min_price
    FROM products
    """)

    conn.commit()

    logger.info("Analytics table created")

    cursor.close()
    conn.close()


with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    quality_task = PythonOperator(
        task_id="data_quality",
        python_callable=data_quality
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    analytics_task = PythonOperator(
        task_id="analytics",
        python_callable=analytics
    )

    extract_task >> transform_task >> quality_task >> load_task >> analytics_task