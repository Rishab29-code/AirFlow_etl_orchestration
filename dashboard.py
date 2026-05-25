import streamlit as st
import pandas as pd
import psycopg2

st.title("ETL Product Dashboard")

conn = psycopg2.connect(
    host="127.0.0.1",
    database="airflow",
    user="airflow",
    password="airflow",
    port="5433"
)

summary_df = pd.read_sql(
    "SELECT * FROM product_summary",
    conn
)

products_df = pd.read_sql(
    "SELECT * FROM products",
    conn
)

# KPI cards
st.metric(
    "Total Products",
    int(summary_df["total_products"][0])
)

st.metric(
    "Average Price",
    float(summary_df["avg_price"][0])
)

# Table
st.subheader("Product Summary")
st.dataframe(summary_df)

# Chart
st.subheader("Top 10 Product Prices")

top_products = products_df.sort_values(
    by="price",
    ascending=False
).head(10)

st.bar_chart(
    top_products.set_index("title")["price"]
)

conn.close()