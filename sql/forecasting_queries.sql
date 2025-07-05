-- FORECASTING & TIME-SERIES ANALYSIS TEMPLATES FOR BIGQUERY

-- 🔍 1. NULL CHECK
SELECT
  column_name,
  COUNTIF(column_name IS NULL) AS null_count
FROM `project_id.dataset.your_table`
GROUP BY column_name;

-- 🔍 2. DUPLICATE CHECK
SELECT
  column1, column2, COUNT(*) AS duplicate_count
FROM `project_id.dataset.your_table`
GROUP BY column1, column2
HAVING duplicate_count > 1;

-- 📊 3. DESCRIPTIVE STATS
SELECT
  MIN(spend) AS min_spend,
  MAX(spend) AS max_spend,
  AVG(spend) AS avg_spend,
  APPROX_QUANTILES(spend, 100)[SAFE_OFFSET(50)] AS median_spend,
  STDDEV(spend) AS std_spend
FROM `project_id.dataset.your_table`;

-- 🏷️ 4. CATEGORICAL DISTRIBUTION
SELECT
  facility_name,
  COUNT(*) AS count
FROM `project_id.dataset.your_table`
GROUP BY facility_name
ORDER BY count DESC;

-- 📈 5. MONTHLY SPEND
SELECT
  EXTRACT(YEAR FROM date_column) AS year,
  EXTRACT(MONTH FROM date_column) AS month,
  SUM(spend) AS monthly_spend
FROM `project_id.dataset.your_table`
GROUP BY year, month
ORDER BY year, month;

-- 🔄 6. ROLLING 3-MONTH AVERAGE
SELECT
  facility_name,
  purchase_date,
  purchase_total,
  AVG(purchase_total) OVER (
    PARTITION BY facility_name
    ORDER BY purchase_date
    RANGE BETWEEN INTERVAL 2 MONTH PRECEDING AND CURRENT ROW
  ) AS rolling_3_month_avg
FROM `project_id.dataset.your_table`
ORDER BY facility_name, purchase_date;

-- 📉 7. CUMULATIVE SPEND
SELECT
  facility_id,
  date_column,
  SUM(spend) OVER (
    PARTITION BY facility_id
    ORDER BY date_column
  ) AS cumulative_spend
FROM `project_id.dataset.your_table`
ORDER BY facility_id, date_column;

-- 📊 8. MONTHLY SPEND HEATMAP PREP
SELECT
  EXTRACT(YEAR FROM date_column) AS year,
  EXTRACT(MONTH FROM date_column) AS month,
  SUM(spend) AS total_monthly_spend
FROM `project_id.dataset.your_table`
GROUP BY year, month
ORDER BY year, month;

-- 📉 9. HISTOGRAM BINS
SELECT
  CAST(FLOOR(spend / 1000) * 1000 AS INT64) AS spend_bin,
  COUNT(*) AS count
FROM `project_id.dataset.your_table`
GROUP BY spend_bin
ORDER BY spend_bin;

-- 📐 10. COHORT ANALYSIS
WITH facility_cohort AS (
  SELECT
    facility_id,
    DATE_TRUNC(MIN(spend_date), MONTH) AS cohort_month
  FROM `project_id.dataset.your_table`
  GROUP BY facility_id
),
monthly_spend AS (
  SELECT
    facility_id,
    DATE_TRUNC(spend_date, MONTH) AS spend_month,
    SUM(spend_amount) AS monthly_spend
  FROM `project_id.dataset.your_table`
  GROUP BY facility_id, spend_month
),
cohort_analysis AS (
  SELECT
    fc.cohort_month,
    ms.spend_month,
    fc.facility_id,
    ms.monthly_spend
  FROM facility_cohort fc
  JOIN monthly_spend ms ON fc.facility_id = ms.facility_id
  WHERE ms.spend_month >= fc.cohort_month
),
cohort_spend AS (
  SELECT
    cohort_month,
    DATE_DIFF(spend_month, cohort_month, MONTH) AS months_since_start,
    SUM(monthly_spend) AS total_monthly_spend
  FROM cohort_analysis
  GROUP BY cohort_month, months_since_start
)
SELECT
  cohort_month,
  months_since_start,
  total_monthly_spend
FROM cohort_spend
ORDER BY cohort_month, months_since_start;

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSUploadFileOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data_team',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'external_model_forecast_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    tags=['forecasting', 'external-model']
) as dag:

    train_model = BashOperator(
        task_id='train_and_forecast_model',
        bash_command='Rscript /path/to/your_forecasting_script.R'
    )

    upload_to_gcs = GCSUploadFileOperator(
        task_id='upload_to_gcs',
        bucket_name='your_bucket_name',
        source_file='/path/to/forecast_data.csv',
        destination_blob_name='forecast_data/forecast_data.csv'
    )

    load_to_bigquery = BigQueryExecuteQueryOperator(
        task_id='load_to_bigquery',
        sql="""
        CREATE OR REPLACE TABLE `your_project.your_dataset.forecast_results` AS
        SELECT * FROM `your_project.your_dataset.temp_forecast_data`;
        """,
        use_legacy_sql=False
    )

    train_model >> upload_to_gcs >> load_to_bigquery
