from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2


def extract():

    response = requests.get(
        "https://dummyjson.com/products"
    )

    data = response.json()

    df = pd.DataFrame(
        data["products"]
    )

    df.to_csv(
        "/opt/airflow/extracted_data.csv",
        index=False
    )

    print("API data extracted")


def transform():

    df = pd.read_csv(
        "/opt/airflow/extracted_data.csv"
    )

    df = df[
        ["id","title","price","rating"]
    ]

    df["price"] = df["price"] * 1.1

    df.to_csv(
        "/opt/airflow/transformed_data.csv",
        index=False
    )

    print("Data transformed")


def load():

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products(
        id INT,
        title VARCHAR(255),
        price FLOAT,
        rating FLOAT
    )
    """)

    df = pd.read_csv(
        "/opt/airflow/transformed_data.csv"
    )

    for _, row in df.iterrows():

        cursor.execute(
        """
        INSERT INTO products
        VALUES (%s,%s,%s,%s)
        """,
        (
            int(row["id"]),
            row["title"],
            float(row["price"]),
            float(row["rating"])
        )
        )

    conn.commit()

    cursor.close()
    conn.close()

    print("Loaded into PostgreSQL")

def analytics():

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS product_summary AS
    SELECT
        COUNT(*) AS total_products,
        ROUND(AVG(price),2) AS avg_price,
        ROUND(MAX(price),2) AS max_price,
        ROUND(MIN(price),2) AS min_price
    FROM products
    """)

    conn.commit()

    cursor.close()
    conn.close()

    print("Analytics table created")


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

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    extract_task >> transform_task >> load_task