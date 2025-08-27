# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE delta_catalog.gold.gold_retention_cohorts (
# MAGIC   cohort_month DATE,
# MAGIC   cohort_size INT,
# MAGIC   period_name STRING,
# MAGIC   period_number INT,
# MAGIC   period_date DATE,
# MAGIC   retained_users INT,
# MAGIC   retention_rate DECIMAL(5,4),
# MAGIC   cohort_type STRING  -- 'subscription' or 'engagement'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@saasanalyticsdls.dfs.core.windows.net/gold_retention_cohorts'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'false'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Step 2: Populate the external Delta table with subscription-based retention data
# MAGIC WITH 
# MAGIC -- Define customer cohorts based on signup month
# MAGIC customer_cohorts AS (
# MAGIC   SELECT DISTINCT
# MAGIC     customer_id,
# MAGIC     DATE_TRUNC('month', CAST(created_at AS DATE)) AS cohort_month,
# MAGIC     CAST(created_at AS DATE) AS signup_date
# MAGIC   FROM delta_catalog.silver.customers_scd2
# MAGIC   WHERE created_at IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC -- Get cohort sizes
# MAGIC cohort_sizes AS (
# MAGIC   SELECT 
# MAGIC     cohort_month,
# MAGIC     COUNT(DISTINCT customer_id) AS cohort_size
# MAGIC   FROM customer_cohorts
# MAGIC   GROUP BY cohort_month
# MAGIC ),
# MAGIC
# MAGIC -- Define retention periods (W1, M1, M3, M6, M12)
# MAGIC retention_periods AS (
# MAGIC   SELECT period_name, period_number, period_months FROM VALUES
# MAGIC     ('W1', 1, 0.25),   -- 1 week ≈ 0.25 months
# MAGIC     ('M1', 2, 1),      -- 1 month
# MAGIC     ('M3', 3, 3),      -- 3 months
# MAGIC     ('M6', 4, 6),      -- 6 months
# MAGIC     ('M12', 5, 12)     -- 12 months
# MAGIC   AS t(period_name, period_number, period_months)
# MAGIC ),
# MAGIC
# MAGIC -- Cross join cohorts with periods to get all combinations
# MAGIC cohort_period_matrix AS (
# MAGIC   SELECT 
# MAGIC     cc.cohort_month,
# MAGIC     cs.cohort_size,
# MAGIC     rp.period_name,
# MAGIC     rp.period_number,
# MAGIC     DATE_ADD(cc.cohort_month, CAST(rp.period_months * 30 AS INT)) AS period_date,
# MAGIC     cc.customer_id
# MAGIC   FROM customer_cohorts cc
# MAGIC   CROSS JOIN retention_periods rp
# MAGIC   JOIN cohort_sizes cs ON cs.cohort_month = cc.cohort_month
# MAGIC   WHERE DATE_ADD(cc.cohort_month, CAST(rp.period_months * 30 AS INT)) <= CURRENT_DATE()
# MAGIC ),
# MAGIC
# MAGIC -- Check retention using subscription status
# MAGIC subscription_retention AS (
# MAGIC   SELECT 
# MAGIC     cpm.cohort_month,
# MAGIC     cpm.cohort_size,
# MAGIC     cpm.period_name,
# MAGIC     cpm.period_number,
# MAGIC     cpm.period_date,
# MAGIC     COUNT(DISTINCT CASE WHEN fsd.status_at_day = 'active' THEN cpm.customer_id END) AS retained_users
# MAGIC   FROM cohort_period_matrix cpm
# MAGIC   LEFT JOIN delta_catalog.silver.fact_subscription_daily fsd
# MAGIC     ON fsd.customer_id = cpm.customer_id
# MAGIC     AND fsd.dt = cpm.period_date
# MAGIC   GROUP BY 
# MAGIC     cpm.cohort_month,
# MAGIC     cpm.cohort_size,
# MAGIC     cpm.period_name,
# MAGIC     cpm.period_number,
# MAGIC     cpm.period_date
# MAGIC )
# MAGIC
# MAGIC INSERT OVERWRITE delta_catalog.gold.gold_retention_cohorts
# MAGIC SELECT 
# MAGIC   cohort_month,
# MAGIC   cohort_size,
# MAGIC   period_name,
# MAGIC   period_number,
# MAGIC   period_date,
# MAGIC   retained_users,
# MAGIC   CASE 
# MAGIC     WHEN cohort_size > 0 THEN CAST(retained_users AS DECIMAL(10,4)) / CAST(cohort_size AS DECIMAL(10,4))
# MAGIC     ELSE 0
# MAGIC   END AS retention_rate,
# MAGIC   'subscription' AS cohort_type
# MAGIC FROM subscription_retention
# MAGIC WHERE cohort_month IS NOT NULL
# MAGIC ORDER BY cohort_month, period_number;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.gold.gold_retention_cohorts

# COMMAND ----------

