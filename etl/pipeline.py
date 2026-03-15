"""
ETL Pipeline — Retail Intelligence Platform
============================================
Extracts raw CSV → validates → cleans → enriches → loads into SQLite.
Designed to be re-runnable (idempotent).
Author: Varun Kumar Jothi
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import RAW_CSV, CLEANED_CSV, DB_PATH, DATA_DIR, REPORTS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


def extract(filepath: str) -> pd.DataFrame:
    log.info(f"EXTRACT — loading {filepath}")
    df = pd.read_csv(filepath)
    log.info(f"  Rows loaded    : {len(df):,}")
    log.info(f"  Columns        : {list(df.columns)}")
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    log.info("VALIDATE — checking data quality")
    issues = []

    required = ["order_id","order_date","customer_id","product_name",
                "quantity","unit_price","discount","sales","profit"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    null_counts = df[required].isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if not null_cols.empty:
        issues.append(f"Nulls found: {null_cols.to_dict()}")

    neg_sales = (df['sales'] < 0).sum()
    if neg_sales:
        issues.append(f"{neg_sales} rows with negative sales")

    dupes = df.duplicated(subset='order_id').sum()
    if dupes:
        issues.append(f"{dupes} duplicate order_ids")

    for issue in issues:
        log.warning(f"  DATA ISSUE: {issue}")

    log.info(f"  Validation complete — {len(issues)} issues flagged")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("TRANSFORM — cleaning and enriching data")

    df['order_date']    = pd.to_datetime(df['order_date'])
    df['order_year']    = df['order_date'].dt.year
    df['order_month']   = df['order_date'].dt.month
    df['order_quarter'] = df['order_date'].dt.quarter
    df['order_week']    = df['order_date'].dt.isocalendar().week.astype(int)
    df['order_dow']     = df['order_date'].dt.day_name()

    df['discount'] = df['discount'].fillna(0).clip(0, 1)
    df['profit']   = df['profit'].fillna(0)

    before = len(df)
    df = df.drop_duplicates(subset='order_id')
    df = df[df['sales'] > 0]
    df = df[df['quantity'] > 0]
    log.info(f"  Rows removed (dupes/invalid): {before - len(df)}")

    for col in ['region','segment','product_category','sales_rep']:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    df['profit_margin_%']  = ((df['profit'] / df['sales']) * 100).round(2)
    df['revenue_per_unit'] = (df['sales'] / df['quantity']).round(2)
    df['is_discounted']    = (df['discount'] > 0).astype(int)
    df['is_profitable']    = (df['profit'] > 0).astype(int)

    latest_date = df['order_date'].max()
    rfm = df.groupby('customer_id').agg(
        recency_days=('order_date', lambda x: (latest_date - x.max()).days),
        frequency=('order_id', 'nunique'),
        monetary=('sales', 'sum')
    ).reset_index()
    df = df.merge(rfm, on='customer_id', how='left')

    log.info(f"  Final row count: {len(df):,}")
    return df


def load_to_db(df: pd.DataFrame, db_path: str):
    log.info(f"LOAD — writing to SQLite: {db_path}")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)

    df_db = df.copy()
    df_db['order_date'] = df_db['order_date'].dt.strftime('%Y-%m-%d')
    df_db.to_sql('sales', conn, if_exists='replace', index=False)

    conn.execute("""
        CREATE VIEW IF NOT EXISTS monthly_kpi AS
        SELECT
            order_year, order_month,
            COUNT(DISTINCT order_id)               AS total_orders,
            ROUND(SUM(sales), 2)                   AS total_revenue,
            ROUND(SUM(profit), 2)                  AS total_profit,
            ROUND(AVG("profit_margin_%"), 2)        AS avg_margin_pct,
            COUNT(DISTINCT customer_id)            AS unique_customers
        FROM sales
        GROUP BY order_year, order_month
    """)

    conn.execute("""
        CREATE VIEW IF NOT EXISTS category_kpi AS
        SELECT
            product_category,
            COUNT(DISTINCT order_id)               AS total_orders,
            SUM(quantity)                          AS units_sold,
            ROUND(SUM(sales), 2)                   AS total_revenue,
            ROUND(SUM(profit), 2)                  AS total_profit,
            ROUND(AVG("profit_margin_%"), 2)        AS avg_margin_pct
        FROM sales
        GROUP BY product_category
        ORDER BY total_revenue DESC
    """)

    conn.execute("""
        CREATE VIEW IF NOT EXISTS rep_performance AS
        SELECT
            sales_rep,
            COUNT(DISTINCT order_id)               AS orders_closed,
            COUNT(DISTINCT customer_id)            AS customers_served,
            ROUND(SUM(sales), 2)                   AS total_revenue,
            ROUND(SUM(profit), 2)                  AS total_profit,
            ROUND(AVG("profit_margin_%"), 2)        AS avg_margin_pct
        FROM sales
        GROUP BY sales_rep
        ORDER BY total_revenue DESC
    """)

    conn.commit()
    conn.close()
    log.info(f"  Loaded {len(df):,} rows into 'sales' table + 3 views")


def load_to_csv(df: pd.DataFrame):
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(CLEANED_CSV, index=False)
    log.info(f"  Cleaned CSV saved: {CLEANED_CSV}")


def run_pipeline():
    log.info("=" * 55)
    log.info("  RETAIL ETL PIPELINE STARTING")
    log.info("=" * 55)
    start = datetime.now()

    df = extract(RAW_CSV)
    df = validate(df)
    df = transform(df)
    load_to_db(df, DB_PATH)
    load_to_csv(df)

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"Pipeline completed in {elapsed:.1f}s — {len(df):,} rows processed")
    return df


if __name__ == "__main__":
    run_pipeline()