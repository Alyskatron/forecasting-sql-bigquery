# Forecasting SQL & Time-Series Analysis in BigQuery

This project provides SQL templates and Airflow pipelines for forecasting, cohort analysis, seasonality exploration, and anomaly detection using Google BigQuery.

---

## 📂 Files Included

| File | Description |
|------|-------------|
| `sql/forecasting_queries.sql` | BigQuery SQL templates for exploratory analysis and forecasting |
| `example_inputs/sample_purchase_history.csv` | Example input dataset for testing the SQL |
| `LICENSE` | MIT License allowing reuse and modification |

---

## 📊 Features

- Null checks and data quality audits
- Time-series decomposition (trend, seasonality, residual)
- Rolling averages & cumulative trends
- Year-over-year growth & cohort modeling
- Outlier and distribution analysis
- Integration with Airflow for automated forecasting pipeline

---

## 🚀 How to Use

1. Replace `project_id.dataset.your_table` in the SQL file with your actual table path.
2. Load `sample_purchase_history.csv` into your BigQuery dataset (optional for testing).
3. Run queries directly in BigQuery UI or schedule with Airflow (DAG included).

---

## 🧪 Sample Query

```sql
-- Monthly spend trend
SELECT
  EXTRACT(YEAR FROM purchase_date) AS year,
  EXTRACT(MONTH FROM purchase_date) AS month,
  SUM(purchase_total) AS monthly_spend
FROM `your_project.your_dataset.purchase_history`
GROUP BY year, month
ORDER BY year, month;
