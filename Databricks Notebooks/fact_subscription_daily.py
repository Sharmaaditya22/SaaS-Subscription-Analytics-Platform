# Databricks notebook source
# MAGIC %sql
# MAGIC -- Step 1: Create the external Delta table with explicit location
# MAGIC CREATE TABLE delta_catalog.silver.fact_subscription_daily (
# MAGIC   dt DATE,
# MAGIC   subscription_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   plan_id STRING,
# MAGIC   status_at_day STRING,
# MAGIC   plan_name STRING,
# MAGIC   tier_at_day STRING,
# MAGIC   monthly_price_at_day DECIMAL(10,2),
# MAGIC   is_active INT,
# MAGIC   is_trialing INT,
# MAGIC   is_canceled INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@saasanalyticsdls.dfs.core.windows.net/fact_subscription_daily'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'false'
# MAGIC );
# MAGIC
# MAGIC -- Step 2: Populate the external Delta table with data
# MAGIC WITH params AS (
# MAGIC   SELECT current_date() AS max_dt
# MAGIC ),
# MAGIC
# MAGIC -- Normalize SCD2 windows
# MAGIC sub AS (
# MAGIC   SELECT
# MAGIC     subscription_id,
# MAGIC     customer_id,
# MAGIC     plan_id,
# MAGIC     status,
# MAGIC     CAST(start_date AS DATE) AS valid_from,
# MAGIC     CAST(COALESCE(end_date, (SELECT max_dt FROM params)) AS DATE) AS valid_to
# MAGIC   FROM delta_catalog.silver.subscriptions_scd2
# MAGIC ),
# MAGIC
# MAGIC -- Expand each subscription SCD2 window to daily rows
# MAGIC sub_daily AS (
# MAGIC   SELECT
# MAGIC     s.subscription_id,
# MAGIC     s.customer_id,
# MAGIC     s.plan_id,
# MAGIC     s.status AS status_at_day,
# MAGIC     dt AS dt
# MAGIC   FROM sub s
# MAGIC   LATERAL VIEW explode(sequence(s.valid_from, s.valid_to, interval 1 day)) AS dt
# MAGIC ),
# MAGIC
# MAGIC -- Attach plan attributes as-of the day
# MAGIC sub_daily_enriched AS (
# MAGIC   SELECT
# MAGIC     sd.dt,
# MAGIC     sd.subscription_id,
# MAGIC     sd.customer_id,
# MAGIC     sd.plan_id,
# MAGIC     sd.status_at_day,
# MAGIC     p.plan_name,
# MAGIC     p.tier         AS tier_at_day,
# MAGIC     p.monthly_price AS monthly_price_at_day
# MAGIC   FROM sub_daily sd
# MAGIC   LEFT JOIN delta_catalog.silver.plans_scd2 p
# MAGIC     ON p.plan_id = sd.plan_id
# MAGIC    AND sd.dt BETWEEN CAST(p.effective_from AS DATE)
# MAGIC                  AND CAST(COALESCE(p.effective_to, DATE '9999-12-31') AS DATE)
# MAGIC )
# MAGIC
# MAGIC INSERT OVERWRITE delta_catalog.silver.fact_subscription_daily
# MAGIC SELECT
# MAGIC   dt,
# MAGIC   subscription_id,
# MAGIC   customer_id,
# MAGIC   plan_id,
# MAGIC   status_at_day,
# MAGIC   plan_name,
# MAGIC   tier_at_day,
# MAGIC   monthly_price_at_day,
# MAGIC   /* convenience flags (optional) */
# MAGIC   CASE WHEN status_at_day = 'active'    THEN 1 ELSE 0 END AS is_active,
# MAGIC   CASE WHEN status_at_day = 'trialing'  THEN 1 ELSE 0 END AS is_trialing,
# MAGIC   CASE WHEN status_at_day = 'canceled'  THEN 1 ELSE 0 END AS is_canceled
# MAGIC FROM sub_daily_enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.silver.fact_subscription_daily 

# COMMAND ----------

