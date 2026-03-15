"""
Master Runner — Retail Intelligence Platform
Runs the full pipeline: Data → ETL → ML → Report
Author: Varun Kumar Jothi
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

print("\n" + "="*60)
print("  RETAIL INTELLIGENCE PLATFORM")
print("  Full Pipeline Run")
print("="*60)

print("\n[1/4] Generating dataset...")
from etl.generate_data import *

print("\n[2/4] Running ETL pipeline...")
from etl.pipeline import run_pipeline
df = run_pipeline()

print("\n[3/4] Training ML model & generating forecast...")
from ml.forecaster import run_forecaster
model, forecast_df, metrics = run_forecaster()

print("\n[4/4] Generating reports & charts...")
from reports.report_generator import run_report
run_report()

print("\n" + "="*60)
print("  ALL DONE! Check the reports/ folder.")
print("  To start the API: python api/app.py")
print("="*60 + "\n")