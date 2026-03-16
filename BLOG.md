# How I Built a Retail Intelligence Platform That Actually Works

*By Varun Kumar Jothi · March 2025*

---

I'm not going to pretend this was easy.

When I decided to build a proper data project — not a tutorial copy-paste, not a Kaggle notebook — I wanted something that felt real. Something I could open in front of a hiring manager and say "I built this, and here's exactly how it works."

That's where the Retail Intelligence Platform came from.

---

## Where It Started

After finishing my M.Sc. in Data Science at Nottingham Trent University, I came back to Chennai with a degree, some theoretical knowledge, and zero production-level projects to show for it. Job descriptions kept asking for ETL experience, API development, ML models. I had the concepts in my head but nothing on GitHub.

So I decided to build something that covered all of it. One project. End to end. No shortcuts.

---

## What I Actually Built

The idea was simple: take raw retail sales data and turn it into a full business intelligence system — automatically.

I broke it into four layers:

**Layer 1 — ETL Pipeline**

This was the foundation. Extract the raw CSV, validate it (check for nulls, duplicates, negative sales values), clean it, enrich it with new columns like profit margin percentage and RFM customer segments, then load it into a SQLite database.

The part I'm most proud of here is the validation step. Most beginner projects skip this completely. But in a real company, bad data flowing into your system causes wrong KPIs and wrong business decisions. I built it to flag issues before they become problems.

The whole pipeline runs in 0.1 seconds for 3,680 records. That felt good.

**Layer 2 — SQL Analytics**

Once the data was in SQLite, I built 8 query functions covering everything a business analyst would actually need: overall KPIs, monthly revenue trends, top products, region and segment breakdowns, sales rep leaderboards, discount impact analysis, and customer RFM segmentation.

Writing these queries made me realise how much SQL I had been underusing. Window functions, CASE statements, HAVING clauses — it all came together here.

**Layer 3 — Machine Learning Forecasting**

This was the most challenging and most rewarding part.

I trained three models — Random Forest, Gradient Boosting, and Ridge Regression — on weekly revenue data and compared them by RMSE. Gradient Boosting won with R²=0.887. That means the model explains 88.7% of the variance in weekly revenue. For a project built on synthetic data, that's a result I'm genuinely proud of.

The forecasting system generates 8 weeks of predictions recursively — each week's prediction feeds into the next one as input. It's the same pattern used in production forecasting systems.

**Layer 4 — Flask REST API**

The final layer was building a 10-endpoint REST API that exposes all the analytics and forecasts as JSON. I also added a secure `/api/query` endpoint that lets you run any SELECT query against the database — but blocks anything that isn't a SELECT.

Building the API made the whole project feel real. It's not just a script anymore. It's a service.

---

## What Went Wrong (Because Something Always Does)

The ML forecaster file had a typo in the filename — `forcaster.py` instead of `forecaster.py`. Spent 20 minutes wondering why Python couldn't find the module before I ran `ls ml/` and saw it staring back at me.

The chart labels on the revenue trend were completely overlapping because I had 24 months of data on the X-axis. Fixed it by limiting the tick locator and rotating the labels.

The profit margin pie chart had two tiny slices (Groceries at 0.4% and Office Supplies at 0.7%) cramming their percentage labels on top of each other. Fixed it by only showing labels on slices bigger than 5% and putting the rest in the legend.

These small fixes taught me more than any tutorial.

---

## What I Learned

Building this project in a modular way — separate files for ETL, ML, SQL, API, reports — taught me how real software is actually structured. Every module has one job. You can test each part independently. You can swap out the database without touching the API. That's good software design.

Writing 8 unit tests also changed how I think about code. Instead of running the script and hoping it works, I now think about what could go wrong and test for it explicitly.

---

## The Result

A system that takes raw sales data and automatically:
- Cleans and validates it
- Runs 8 business analytics queries
- Trains and compares 3 ML models
- Generates an 8-week forecast
- Serves everything through a REST API
- Produces 5 charts and a 6-sheet Excel report

All in one command: `python run_all.py`

---

## Try It Yourself

The full code is on GitHub:
**https://github.com/varunkumarjothi/Retail-Intelligence-Platform**

The Tableau dashboard is live:
**https://public.tableau.com/app/profile/varun.kumar.jothi/viz/RetailIntelligencePlatform/RetailIntelligenceDashboard**

---

*If you're building something similar or just want to talk data — reach out at varunkumarjothi@gmail.com*
