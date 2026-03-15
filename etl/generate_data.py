"""
Data Generator — creates a realistic multi-year retail dataset.
Run once to populate data/raw_sales.csv
"""
import pandas as pd
import numpy as np
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import RAW_CSV, DATA_DIR

np.random.seed(42)

PRODUCTS = [
    ("Laptop Pro 15",      "Electronics",    72000, 0.55),
    ("Smartphone X12",     "Electronics",    28000, 0.52),
    ("Wireless Earbuds",   "Electronics",     3800, 0.48),
    ("Smart TV 55inch",    "Electronics",    58000, 0.50),
    ("USB-C Hub",          "Electronics",     2600, 0.45),
    ("Sofa Set 3-Seater",  "Furniture",      44000, 0.42),
    ("Office Chair Ergo",  "Furniture",      12500, 0.40),
    ("Standing Desk",      "Furniture",      22000, 0.38),
    ("Bookshelf Walnut",   "Furniture",       7200, 0.35),
    ("Printer Paper A4",   "Office Supplies",  380, 0.60),
    ("Whiteboard Markers", "Office Supplies",  160, 0.55),
    ("Stapler Heavy Duty", "Office Supplies",  420, 0.50),
    ("Premium Rice 10kg",  "Groceries",        680, 0.30),
    ("Cooking Oil 5L",     "Groceries",        520, 0.28),
    ("Basmati Rice 5kg",   "Groceries",        440, 0.29),
]

REGIONS     = ["South", "North", "East", "West"]
SEGMENTS    = ["Corporate", "Small Business", "Consumer"]
SALES_REPS  = ["Anitha R", "Manoj K", "Priya S", "Ramesh T", "Divya N", "Karthik B"]
CUSTOMERS   = [f"C{str(i).zfill(3)}" for i in range(1, 51)]
CUST_NAMES  = [
    "Arun Sharma","Meena Patel","Sundar Raj","Karthik B","Divya Nair",
    "Ravi Menon","Lakshmi V","Vijay T","Shalini K","Preethi S",
    "Rohit Gupta","Ananya Sen","Suresh Kumar","Nithya R","Bala M",
    "Kavitha P","Sanjay D","Pooja A","Arjun Nair","Sneha G",
    "Harish T","Deepa S","Mani K","Jaya R","Praveen B",
    "Rekha M","Gopal V","Saranya N","Dinesh P","Mala S",
    "Venkat R","Chitra K","Rajan A","Usha B","Siva T",
    "Geetha M","Anil S","Radha P","Binu K","Leela N",
    "Sunil A","Prema V","Raja G","Shanthi R","Mohan T",
    "Kamala P","Sathish K","Vani M","Balaji R","Hema S"
]

records = []
order_num = 1000

dates = pd.date_range("2023-01-02", "2024-12-30", freq="D")

for date in dates:
    n_orders = np.random.randint(2, 9)
    for _ in range(n_orders):
        prod_name, category, base_price, cost_ratio = PRODUCTS[np.random.randint(0, len(PRODUCTS))]

        month = date.month
        seasonal = 1.0 + 0.3 * np.sin((month - 3) * np.pi / 6)

        qty = max(1, int(np.random.poisson(3) * seasonal))
        discount = round(np.random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20],
                                           p=[0.4,0.15,0.15,0.1,0.1,0.05,0.05]), 2)
        unit_price = round(base_price * (1 + np.random.uniform(-0.05, 0.05)), 0)
        sales      = round(qty * unit_price * (1 - discount), 2)
        cost       = round(sales * cost_ratio, 2)
        profit     = round(sales - cost, 2)
        margin     = round((profit / sales) * 100, 2) if sales > 0 else 0

        cust_idx   = np.random.randint(0, 50)
        records.append({
            "order_id":        f"ORD-{order_num}",
            "order_date":      date.strftime("%Y-%m-%d"),
            "customer_id":     CUSTOMERS[cust_idx],
            "customer_name":   CUST_NAMES[cust_idx],
            "region":          np.random.choice(REGIONS),
            "segment":         np.random.choice(SEGMENTS),
            "sales_rep":       np.random.choice(SALES_REPS),
            "product_name":    prod_name,
            "product_category":category,
            "quantity":        qty,
            "unit_price":      unit_price,
            "discount":        discount,
            "sales":           sales,
            "cost":            cost,
            "profit":          profit,
            "profit_margin_%": margin,
        })
        order_num += 1

df = pd.DataFrame(records)
os.makedirs(DATA_DIR, exist_ok=True)
df.to_csv(RAW_CSV, index=False)
print(f"Generated {len(df):,} records → {RAW_CSV}")
print(f"Date range : {df['order_date'].min()} to {df['order_date'].max()}")
print(f"Total revenue: Rs.{df['sales'].sum():,.0f}")