# Databricks notebook source
# MAGIC %sql
# MAGIC -- Step 1: Create the external Delta table structure
# MAGIC CREATE TABLE delta_catalog.gold.gold_kpi_daily (
# MAGIC   dt DATE,
# MAGIC   mrr DECIMAL(15,2),
# MAGIC   new_mrr DECIMAL(15,2),
# MAGIC   expansion_mrr DECIMAL(15,2),
# MAGIC   contraction_mrr DECIMAL(15,2),
# MAGIC   churn_mrr DECIMAL(15,2),
# MAGIC   active_customers BIGINT,
# MAGIC   arr DECIMAL(15,2),
# MAGIC   arpu DECIMAL(10,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@saasanalyticsdls.dfs.core.windows.net/gold_kpi_daily'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'false'
# MAGIC );
# MAGIC
# MAGIC -- Step 2: Populate the external Delta table with data
# MAGIC WITH snap AS (
# MAGIC   SELECT
# MAGIC     dt,
# MAGIC     subscription_id,
# MAGIC     customer_id,
# MAGIC     plan_id,
# MAGIC     status_at_day,
# MAGIC     monthly_price_at_day,
# MAGIC     LAG(status_at_day) OVER (PARTITION BY subscription_id ORDER BY dt)    AS prev_status,
# MAGIC     LAG(monthly_price_at_day) OVER (PARTITION BY subscription_id ORDER BY dt) AS prev_price
# MAGIC   FROM delta_catalog.silver.fact_subscription_daily
# MAGIC ),
# MAGIC -- 2) Per-subscription daily deltas (what changed today)
# MAGIC deltas AS (
# MAGIC   SELECT
# MAGIC     dt,
# MAGIC     subscription_id,
# MAGIC     /* Active today */
# MAGIC     CASE WHEN status_at_day = 'active' THEN 1 ELSE 0 END AS is_active_today,
# MAGIC     /* New MRR: became paid today (prev not active → active today) */
# MAGIC     CASE WHEN (COALESCE(prev_status,'_NONE_') <> 'active' AND status_at_day = 'active')
# MAGIC          THEN monthly_price_at_day ELSE 0 END AS new_mrr,
# MAGIC     /* Churn MRR: was active yesterday → not active today (use yesterday's price) */
# MAGIC     CASE WHEN (prev_status = 'active' AND status_at_day <> 'active')
# MAGIC          THEN COALESCE(prev_price,0) ELSE 0 END AS churn_mrr,
# MAGIC     /* Expansion: stayed active and price increased (count the delta) */
# MAGIC     CASE WHEN (prev_status = 'active' AND status_at_day = 'active' AND monthly_price_at_day > COALESCE(prev_price,0))
# MAGIC          THEN (monthly_price_at_day - COALESCE(prev_price,0)) ELSE 0 END AS expansion_mrr,
# MAGIC     /* Contraction: stayed active and price decreased (count the delta) */
# MAGIC     CASE WHEN (prev_status = 'active' AND status_at_day = 'active' AND monthly_price_at_day < COALESCE(prev_price,0))
# MAGIC          THEN (COALESCE(prev_price,0) - monthly_price_at_day) ELSE 0 END AS contraction_mrr,
# MAGIC     /* MRR today: price if active today, else 0 */
# MAGIC     CASE WHEN status_at_day = 'active' THEN monthly_price_at_day ELSE 0 END AS mrr_today
# MAGIC   FROM snap
# MAGIC )
# MAGIC -- 3) Roll up to daily KPIs and insert into external table
# MAGIC INSERT OVERWRITE delta_catalog.gold.gold_kpi_daily
# MAGIC SELECT
# MAGIC   dt,
# MAGIC   SUM(mrr_today)                                 AS mrr,
# MAGIC   SUM(new_mrr)                                   AS new_mrr,
# MAGIC   SUM(expansion_mrr)                             AS expansion_mrr,
# MAGIC   SUM(contraction_mrr)                           AS contraction_mrr,
# MAGIC   SUM(churn_mrr)                                 AS churn_mrr,
# MAGIC   SUM(is_active_today)                           AS active_customers,
# MAGIC   /* nice-to-haves */
# MAGIC   SUM(mrr_today) * 12                            AS arr,
# MAGIC   CASE WHEN SUM(is_active_today) > 0
# MAGIC        THEN SUM(mrr_today) / SUM(is_active_today)
# MAGIC        ELSE 0 END                                AS arpu
# MAGIC FROM deltas
# MAGIC GROUP BY dt
# MAGIC ORDER BY dt;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.gold.gold_kpi_daily

# COMMAND ----------

