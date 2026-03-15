"""
Sales Forecasting Engine — Retail Intelligence Platform
=======================================================
Trains a Random Forest model on weekly revenue data,
evaluates it (MAE, RMSE, R²), and generates 8-week ahead forecasts.
Author: Varun Kumar Jothi
"""

import pandas as pd
import numpy as np
import sqlite3
import pickle
import os
import sys
import logging

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit, cross_val_score

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import DB_PATH, MODEL_PATH, FORECAST_WEEKS, TEST_SPLIT

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['week'] = df['order_date'].dt.to_period('W').apply(lambda r: r.start_time)

    weekly = df.groupby('week').agg(
        revenue=('sales', 'sum'),
        orders=('order_id', 'nunique'),
        avg_order_value=('sales', 'mean'),
        unique_customers=('customer_id', 'nunique'),
        avg_margin=('profit_margin_%', 'mean'),
        discounted_orders=('is_discounted', 'sum'),
    ).reset_index().sort_values('week')

    for lag in [1, 2, 3, 4]:
        weekly[f'revenue_lag{lag}'] = weekly['revenue'].shift(lag)
        weekly[f'orders_lag{lag}']  = weekly['orders'].shift(lag)

    weekly['revenue_ma4']  = weekly['revenue'].shift(1).rolling(4).mean()
    weekly['revenue_ma8']  = weekly['revenue'].shift(1).rolling(8).mean()
    weekly['revenue_std4'] = weekly['revenue'].shift(1).rolling(4).std()

    weekly['week_of_year'] = weekly['week'].dt.isocalendar().week.astype(int)
    weekly['month']        = weekly['week'].dt.month
    weekly['quarter']      = weekly['week'].dt.quarter
    weekly['is_q4']        = (weekly['quarter'] == 4).astype(int)

    weekly = weekly.dropna()
    return weekly


def get_feature_cols():
    return [
        'orders', 'avg_order_value', 'unique_customers', 'avg_margin',
        'discounted_orders',
        'revenue_lag1', 'revenue_lag2', 'revenue_lag3', 'revenue_lag4',
        'orders_lag1', 'orders_lag2', 'orders_lag3', 'orders_lag4',
        'revenue_ma4', 'revenue_ma8', 'revenue_std4',
        'week_of_year', 'month', 'quarter', 'is_q4',
    ]


def train_model(weekly: pd.DataFrame):
    features = get_feature_cols()
    X = weekly[features].values
    y = weekly['revenue'].values

    split_idx = int(len(X) * (1 - TEST_SPLIT))
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    log.info(f"Training on {len(X_train)} weeks, testing on {len(X_test)} weeks")

    models = {
        'RandomForest':     RandomForestRegressor(n_estimators=200, max_depth=8,
                                                   min_samples_leaf=3, random_state=42),
        'GradientBoosting': GradientBoostingRegressor(n_estimators=200, learning_rate=0.05,
                                                       max_depth=4, random_state=42),
        'Ridge':            Ridge(alpha=10.0),
    }

    results = {}
    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        mae  = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2   = r2_score(y_test, y_pred)
        results[name] = {'model': model, 'MAE': mae, 'RMSE': rmse, 'R2': r2}
        log.info(f"  {name:<20} MAE={mae:>10,.0f}  RMSE={rmse:>10,.0f}  R²={r2:.3f}")

    best_name = min(results, key=lambda k: results[k]['RMSE'])
    best      = results[best_name]
    log.info(f"\n  Best model: {best_name} (RMSE={best['RMSE']:,.0f}, R²={best['R2']:.3f})")

    if hasattr(best['model'], 'feature_importances_'):
        fi = pd.Series(best['model'].feature_importances_, index=features)
        log.info("\n  Top 5 features:")
        for feat, imp in fi.nlargest(5).items():
            log.info(f"    {feat:<25}: {imp:.3f}")

    return best['model'], best_name, {k: {m: v for m, v in v.items() if m != 'model'}
                                       for k, v in results.items()}


def generate_forecast(model, weekly: pd.DataFrame, n_weeks: int = FORECAST_WEEKS) -> pd.DataFrame:
    features  = get_feature_cols()
    history   = weekly.copy()
    forecasts = []

    for i in range(1, n_weeks + 1):
        last_row  = history.iloc[-1]
        next_week = last_row['week'] + pd.Timedelta(weeks=1)

        row = {
            'orders':            last_row['orders'],
            'avg_order_value':   last_row['avg_order_value'],
            'unique_customers':  last_row['unique_customers'],
            'avg_margin':        last_row['avg_margin'],
            'discounted_orders': last_row['discounted_orders'],
            'revenue_lag1':      history['revenue'].iloc[-1],
            'revenue_lag2':      history['revenue'].iloc[-2] if len(history) > 1 else 0,
            'revenue_lag3':      history['revenue'].iloc[-3] if len(history) > 2 else 0,
            'revenue_lag4':      history['revenue'].iloc[-4] if len(history) > 3 else 0,
            'orders_lag1':       history['orders'].iloc[-1],
            'orders_lag2':       history['orders'].iloc[-2] if len(history) > 1 else 0,
            'orders_lag3':       history['orders'].iloc[-3] if len(history) > 2 else 0,
            'orders_lag4':       history['orders'].iloc[-4] if len(history) > 3 else 0,
            'revenue_ma4':       history['revenue'].iloc[-4:].mean(),
            'revenue_ma8':       history['revenue'].iloc[-8:].mean(),
            'revenue_std4':      history['revenue'].iloc[-4:].std(),
            'week_of_year':      next_week.isocalendar()[1],
            'month':             next_week.month,
            'quarter':           (next_week.month - 1) // 3 + 1,
            'is_q4':             int(next_week.month in [10, 11, 12]),
        }

        X_pred    = np.array([[row[f] for f in features]])
        y_pred    = model.predict(X_pred)[0]
        forecasts.append({'week': next_week, 'forecasted_revenue': round(y_pred, 2)})

        new_row            = pd.Series(row)
        new_row['week']    = next_week
        new_row['revenue'] = y_pred
        history = pd.concat([history, new_row.to_frame().T], ignore_index=True)

    return pd.DataFrame(forecasts)


def run_forecaster():
    log.info("=" * 55)
    log.info("  SALES FORECASTING ENGINE")
    log.info("=" * 55)

    conn   = sqlite3.connect(DB_PATH)
    df     = pd.read_sql("SELECT * FROM sales", conn)
    conn.close()

    weekly = build_features(df)
    log.info(f"Weekly data: {len(weekly)} periods")

    model, model_name, metrics = train_model(weekly)

    forecast_df = generate_forecast(model, weekly)
    log.info(f"\n  {FORECAST_WEEKS}-Week Forecast:")
    log.info(f"  {'Week':<15} {'Forecasted Revenue':>20}")
    log.info("  " + "-" * 37)
    for _, row in forecast_df.iterrows():
        log.info(f"  {str(row['week'].date()):<15} Rs.{row['forecasted_revenue']:>15,.0f}")

    with open(MODEL_PATH, 'wb') as f:
        pickle.dump({'model': model, 'name': model_name,
                     'metrics': metrics, 'features': get_feature_cols()}, f)
    log.info(f"\n  Model saved: {MODEL_PATH}")

    forecast_path = os.path.join(os.path.dirname(DB_PATH), 'forecast.csv')
    forecast_df.to_csv(forecast_path, index=False)
    log.info(f"  Forecast saved: {forecast_path}")

    return model, forecast_df, metrics

if __name__ == "__main__":
    run_forecaster()