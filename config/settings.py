"""
Central configuration for the Retail Intelligence Platform.
All environment settings, paths, and constants live here.
"""
import os

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH     = os.path.join(BASE_DIR, 'data', 'retail.db')
DATA_DIR    = os.path.join(BASE_DIR, 'data')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')
RAW_CSV     = os.path.join(DATA_DIR, 'raw_sales.csv')
CLEANED_CSV = os.path.join(DATA_DIR, 'cleaned_sales.csv')
MODEL_PATH  = os.path.join(DATA_DIR, 'forecast_model.pkl')

FORECAST_WEEKS          = 8
TEST_SPLIT              = 0.2
API_HOST                = '0.0.0.0'
API_PORT                = 5000
LOW_MARGIN_THRESHOLD    = 15.0
HIGH_DISCOUNT_THRESHOLD = 20.0
REPORT_TITLE            = "Retail Intelligence Platform — Business Report"