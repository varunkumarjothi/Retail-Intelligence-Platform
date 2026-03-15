"""
SQL Analytics Layer — Retail Intelligence Platform
====================================================
Advanced SQL queries executed against the SQLite DB.
Results are returned as DataFrames for API / reporting use.
Author: Varun Kumar Jothi
"""

import sqlite3
import pandas as pd
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import DB_PATH


def get_conn():
    return sqlite3.connect(DB_PATH)


def overall_kpis() -> dict:
    conn = get_conn()
    q = """
        SELECT
            COUNT(DISTINCT order_id)                         AS total_orders,
            COUNT(DISTINCT customer_id)                      AS unique_customers,
            ROUND(SUM(sales), 2)                             AS total_revenue,
            ROUND(SUM(profit), 2)                            AS total_profit,
            ROUND(AVG("profit_margin_%"), 2)                 AS avg_margin_pct,
            ROUND(SUM(sales) / COUNT(DISTINCT order_id), 2)  AS avg_order_value,
            ROUND(SUM(CASE WHEN is_discounted=1 THEN sales ELSE 0 END)
                  / SUM(sales) * 100, 2)                     AS discounted_revenue_pct
        FROM sales
    """
    result = pd.read_sql(q, conn).to_dict(orient='records')[0]
    conn.close()
    return result


def revenue_trend_monthly() -> pd.DataFrame:
    conn = get_conn()
    q = """
        SELECT
            order_year, order_month,
            COUNT(DISTINCT order_id)               AS total_orders,
            ROUND(SUM(sales), 2)                   AS revenue,
            ROUND(SUM(profit), 2)                  AS profit,
            ROUND(AVG("profit_margin_%"), 2)        AS avg_margin_pct,
            COUNT(DISTINCT customer_id)            AS unique_customers,
            ROUND(SUM(sales) / COUNT(DISTINCT order_id), 2) AS avg_order_value
        FROM sales
        GROUP BY order_year, order_month
        ORDER BY order_year, order_month
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df


def top_products(limit: int = 10) -> pd.DataFrame:
    conn = get_conn()
    q = f"""
        SELECT
            product_name,
            product_category,
            SUM(quantity)                          AS units_sold,
            COUNT(DISTINCT order_id)               AS order_count,
            ROUND(SUM(sales), 2)                   AS total_revenue,
            ROUND(SUM(profit), 2)                  AS total_profit,
            ROUND(AVG("profit_margin_%"), 2)        AS avg_margin_pct,
            ROUND(AVG(discount) * 100, 1)          AS avg_discount_pct
        FROM sales
        GROUP BY product_name, product_category
        ORDER BY total_revenue DESC
        LIMIT {limit}
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df


def region_segment_matrix() -> pd.DataFrame:
    conn = get_conn()
    q = """
        SELECT
            region, segment,
            COUNT(DISTINCT order_id)               AS orders,
            COUNT(DISTINCT customer_id)            AS customers,
            ROUND(SUM(sales), 2)                   AS revenue,
            ROUND(SUM(profit), 2)                  AS profit,
            ROUND(AVG("profit_margin_%"), 2)        AS avg_margin_pct
        FROM sales
        GROUP BY region, segment
        ORDER BY revenue DESC
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df


def sales_rep_leaderboard() -> pd.DataFrame:
    conn = get_conn()
    q = """
        SELECT
            sales_rep,
            COUNT(DISTINCT order_id)               AS orders_closed,
            COUNT(DISTINCT customer_id)            AS customers_served,
            ROUND(SUM(sales), 2)                   AS total_revenue,
            ROUND(SUM(profit), 2)                  AS total_profit,
            ROUND(AVG("profit_margin_%"), 2)        AS avg_margin_pct,
            ROUND(SUM(sales)/COUNT(DISTINCT order_id),2) AS avg_deal_size
        FROM sales
        GROUP BY sales_rep
        ORDER BY total_revenue DESC
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df


def discount_impact_analysis() -> pd.DataFrame:
    conn = get_conn()
    q = """
        SELECT
            CASE WHEN is_discounted=1 THEN 'Discounted' ELSE 'Full Price' END AS pricing_type,
            ROUND(AVG(discount)*100, 1)            AS avg_discount_pct,
            COUNT(DISTINCT order_id)               AS order_count,
            ROUND(SUM(sales), 2)                   AS total_revenue,
            ROUND(SUM(profit), 2)                  AS total_profit,
            ROUND(AVG("profit_margin_%"), 2)        AS avg_margin_pct
        FROM sales
        GROUP BY is_discounted
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df


def customer_rfm() -> pd.DataFrame:
    conn = get_conn()
    q = """
        SELECT
            customer_id, customer_name,
            recency_days,
            frequency,
            ROUND(monetary, 2) AS monetary,
            CASE
                WHEN recency_days <= 30  AND frequency >= 10 THEN 'Champions'
                WHEN recency_days <= 60  AND frequency >= 5  THEN 'Loyal'
                WHEN recency_days <= 90                      THEN 'At Risk'
                ELSE 'Lost'
            END AS rfm_segment
        FROM (
            SELECT customer_id, customer_name,
                   recency_days, frequency, monetary
            FROM sales
            GROUP BY customer_id, customer_name
        )
        ORDER BY monetary DESC
        LIMIT 30
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df


def low_margin_alerts() -> pd.DataFrame:
    conn = get_conn()
    q = """
        SELECT
            product_name, product_category,
            COUNT(DISTINCT order_id)               AS order_count,
            ROUND(AVG("profit_margin_%"), 2)        AS avg_margin_pct,
            ROUND(SUM(sales), 2)                   AS total_revenue
        FROM sales
        GROUP BY product_name, product_category
        HAVING avg_margin_pct < 20
        ORDER BY avg_margin_pct ASC
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df


if __name__ == "__main__":
    print("\n=== OVERALL KPIs ===")
    for k, v in overall_kpis().items():
        print(f"  {k:<30}: {v}")

    print("\n=== TOP 5 PRODUCTS ===")
    print(top_products(5).to_string(index=False))

    print("\n=== SALES REP LEADERBOARD ===")
    print(sales_rep_leaderboard().to_string(index=False))

    print("\n=== DISCOUNT IMPACT ===")
    print(discount_impact_analysis().to_string(index=False))

    print("\n=== LOW MARGIN ALERTS ===")
    alerts = low_margin_alerts()
    if alerts.empty:
        print("  None found.")
    else:
        print(alerts.to_string(index=False))