# Retail Intelligence Platform

An end-to-end retail analytics platform with automated ETL pipeline, advanced SQL KPI queries, ensemble ML forecasting (R²=0.887), 10-endpoint Flask REST API, RFM customer segmentation and automated Excel reports with charts.

## Live Dashboard
View the interactive Tableau dashboard:
https://public.tableau.com/app/profile/varun.kumar.jothi/viz/RetailIntelligencePlatform/RetailIntelligenceDashboard

## Architecture
```
retail-intelligence-platform/
├── config/settings.py           # Central config
├── etl/generate_data.py         # Dataset generator
├── etl/pipeline.py              # ETL pipeline
├── ml/forecaster.py             # ML forecasting engine
├── sql/analytics.py             # SQL analytics layer
├── api/app.py                   # Flask REST API
├── reports/report_generator.py  # Charts + Excel report
├── tests/test_etl.py            # Unit tests
└── run_all.py                   # Master runner
```

## How to Run
```bash
# Install dependencies
pip install flask pandas numpy scikit-learn matplotlib openpyxl

# Run full pipeline
python run_all.py

# Start the API
python api/app.py

# Run tests
python tests/test_etl.py
```

## Key Results

| KPI | Value |
|-----|-------|
| Total Revenue | Rs. 1,56,90,651 |
| Total Orders | 3,680 |
| Forecast R² | 0.887 |
| Best ML Model | Gradient Boosting |
| API Endpoints | 10 |
| Unit Tests | 8 |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| GET /api/health | Service health check |
| GET /api/kpis | Overall business KPIs |
| GET /api/revenue/trend | Monthly revenue trend |
| GET /api/products/top | Top products by revenue |
| GET /api/regions | Region × segment matrix |
| GET /api/salesreps | Sales rep leaderboard |
| GET /api/customers/rfm | Customer RFM segmentation |
| GET /api/forecast | 8-week ML forecast |
| GET /api/model/info | ML model metrics |
| POST /api/query | Ad-hoc SQL query |

## Skills Demonstrated
Python · ETL · SQLite · SQL · Feature Engineering · Ensemble ML · Flask REST API · Data Validation · Unit Testing · Automated Reporting · Tableau
```
