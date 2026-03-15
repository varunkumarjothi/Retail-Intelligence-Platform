"""
Unit Tests — ETL Pipeline
Author: Varun Kumar Jothi
"""
import sys, os, unittest
import pandas as pd
import numpy as np
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from etl.pipeline import validate, transform

def make_df(n=20):
    np.random.seed(0)
    return pd.DataFrame({
        'order_id':        [f'ORD-{i}' for i in range(n)],
        'order_date':      pd.date_range('2024-01-01', periods=n, freq='D').strftime('%Y-%m-%d'),
        'customer_id':     [f'C{i%10:03d}' for i in range(n)],
        'customer_name':   ['Test Customer'] * n,
        'region':          np.random.choice(['South','North'], n),
        'segment':         np.random.choice(['Corporate','Consumer'], n),
        'sales_rep':       ['Rep A'] * n,
        'product_name':    ['Laptop'] * n,
        'product_category':['Electronics'] * n,
        'quantity':        np.random.randint(1, 5, n),
        'unit_price':      [50000] * n,
        'discount':        np.random.choice([0, 0.05, 0.10], n),
        'sales':           np.random.uniform(40000, 200000, n),
        'cost':            np.random.uniform(20000, 100000, n),
        'profit':          np.random.uniform(5000, 50000, n),
        'profit_margin_%': np.random.uniform(10, 40, n),
    })


class TestValidation(unittest.TestCase):
    def test_validate_passes_clean_data(self):
        df = make_df()
        result = validate(df)
        self.assertEqual(len(result), len(df))

    def test_validate_raises_on_missing_col(self):
        df = make_df().drop(columns=['sales'])
        with self.assertRaises(ValueError):
            validate(df)


class TestTransform(unittest.TestCase):
    def setUp(self):
        self.df = transform(make_df())

    def test_date_columns_added(self):
        for col in ['order_year','order_month','order_quarter','order_week']:
            self.assertIn(col, self.df.columns)

    def test_no_negative_sales(self):
        self.assertTrue((self.df['sales'] > 0).all())

    def test_no_duplicates(self):
        self.assertEqual(self.df['order_id'].nunique(), len(self.df))

    def test_kpi_columns_present(self):
        for col in ['profit_margin_%','revenue_per_unit','is_discounted','is_profitable']:
            self.assertIn(col, self.df.columns)

    def test_discount_clipped(self):
        self.assertTrue((self.df['discount'] >= 0).all())
        self.assertTrue((self.df['discount'] <= 1).all())

    def test_rfm_columns_added(self):
        for col in ['recency_days','frequency','monetary']:
            self.assertIn(col, self.df.columns)


if __name__ == '__main__':
    unittest.main(verbosity=2)