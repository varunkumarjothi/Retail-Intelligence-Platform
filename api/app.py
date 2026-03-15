"""
REST API — Retail Intelligence Platform
========================================
Flask-based REST API exposing analytics, forecasts, and KPIs.
Author: Varun Kumar Jothi
"""

import os
import sys
import json
import pickle
import sqlite3
import pandas as pd
from datetime import datetime
from flask import Flask, jsonify, request, abort

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import DB_PATH, MODEL_PATH, API_HOST, API_PORT
from sql.analytics import (
    overall_kpis, revenue_trend_monthly, top_products,
    region_segment_matrix, sales_rep_leaderboard,
    discount_impact_analysis, customer_rfm, low_margin_alerts
)

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False


def api_response(data, message="success", status=200):
    return jsonify({
        "status":    message,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "data":      data
    }), status


def df_to_records(df: pd.DataFrame):
    return json.loads(df.to_json(orient='records'))


@app.route('/api/health', methods=['GET'])
def health():
    db_ok    = os.path.exists(DB_PATH)
    model_ok = os.path.exists(MODEL_PATH)
    return api_response({
        "service":   "Retail Intelligence Platform",
        "version":   "1.0.0",
        "db_status": "connected" if db_ok else "missing",
        "ml_status": "loaded"    if model_ok else "not trained",
    })


@app.route('/api/kpis', methods=['GET'])
def get_kpis():
    return api_response(overall_kpis())


@app.route('/api/revenue/trend', methods=['GET'])
def get_revenue_trend():
    year = request.args.get('year', type=int)
    df   = revenue_trend_monthly()
    if year:
        df = df[df['order_year'] == year]
    return api_response(df_to_records(df))


@app.route('/api/products/top', methods=['GET'])
def get_top_products():
    limit = request.args.get('limit', default=10, type=int)
    limit = max(1, min(limit, 50))
    return api_response(df_to_records(top_products(limit)))


@app.route('/api/products/alerts', methods=['GET'])
def get_low_margin_alerts():
    return api_response(df_to_records(low_margin_alerts()))


@app.route('/api/regions', methods=['GET'])
def get_regions():
    return api_response(df_to_records(region_segment_matrix()))


@app.route('/api/salesreps', methods=['GET'])
def get_sales_reps():
    return api_response(df_to_records(sales_rep_leaderboard()))


@app.route('/api/discounts', methods=['GET'])
def get_discount_analysis():
    return api_response(df_to_records(discount_impact_analysis()))


@app.route('/api/customers/rfm', methods=['GET'])
def get_customer_rfm():
    return api_response(df_to_records(customer_rfm()))


@app.route('/api/forecast', methods=['GET'])
def get_forecast():
    forecast_csv = os.path.join(os.path.dirname(DB_PATH), 'forecast.csv')
    if not os.path.exists(forecast_csv):
        return api_response({"error": "Forecast not generated. Run ml/forecaster.py first."}, "error", 503)
    df = pd.read_csv(forecast_csv)
    total_forecast = df['forecasted_revenue'].sum()
    return api_response({
        "forecast_weeks": len(df),
        "total_forecasted_revenue": round(total_forecast, 2),
        "weekly_forecast": df_to_records(df)
    })


@app.route('/api/model/info', methods=['GET'])
def get_model_info():
    if not os.path.exists(MODEL_PATH):
        return api_response({"error": "Model not trained yet."}, "error", 503)
    with open(MODEL_PATH, 'rb') as f:
        bundle = pickle.load(f)
    return api_response({
        "model_name":   bundle['name'],
        "features":     bundle['features'],
        "metrics":      bundle['metrics'],
    })


@app.route('/api/query', methods=['POST'])
def run_query():
    body = request.get_json(silent=True)
    if not body or 'sql' not in body:
        abort(400, description="Request body must include 'sql' key.")

    sql = body['sql'].strip()
    if not sql.upper().startswith('SELECT'):
        abort(403, description="Only SELECT queries are permitted.")

    try:
        conn = sqlite3.connect(DB_PATH)
        df   = pd.read_sql(sql, conn)
        conn.close()
        return api_response({
            "rows":    len(df),
            "columns": list(df.columns),
            "results": df_to_records(df)
        })
    except Exception as e:
        return api_response({"error": str(e)}, "error", 400)


@app.errorhandler(404)
def not_found(e):
    return api_response({"error": str(e)}, "not_found", 404)

@app.errorhandler(400)
def bad_request(e):
    return api_response({"error": str(e)}, "bad_request", 400)

@app.errorhandler(403)
def forbidden(e):
    return api_response({"error": str(e)}, "forbidden", 403)


if __name__ == "__main__":
    print("\n Retail Intelligence Platform API")
    print(f" Running on http://{API_HOST}:{API_PORT}")
    print(" Endpoints: /api/health, /api/kpis, /api/revenue/trend,")
    print("            /api/products/top, /api/regions, /api/salesreps,")
    print("            /api/forecast, /api/model/info, /api/query\n")
    app.run(host=API_HOST, port=API_PORT, debug=True)