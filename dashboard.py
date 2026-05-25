import streamlit as st
import pandas as pd
import psycopg2

st.title("ETL Product Dashboard")

try:
    conn = psycopg2.connect(
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow",
        port="5432"
    )

    df = pd.read_sql(
        "SELECT * FROM product_summary",
        conn
    )

    st.dataframe(df)

    conn.close()

except Exception as e:
    st.error(str(e))