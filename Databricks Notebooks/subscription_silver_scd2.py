# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create target table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.silver.subscriptions_scd2
# MAGIC using delta
# MAGIC location 'abfss://silver@saasanalyticsdls.dfs.core.windows.net/subscriptions_scd2'
# MAGIC as
# MAGIC select * from delta_catalog.silver.subscriptions limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta_catalog.silver.subscriptions_scd2
# MAGIC ADD COLUMNS (
# MAGIC     start_date TIMESTAMP,
# MAGIC     end_date TIMESTAMP,
# MAGIC     active_status STRING
# MAGIC );
# MAGIC update delta_catalog.silver.subscriptions_scd2 set start_date=current_timestamp(),active_status='Y'

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta_catalog.silver.subscriptions_scd2 set status='invalid' where status='active'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.silver.subscriptions_scd2

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD 2 Logic

# COMMAND ----------

df_source=spark.read.format('delta').load('abfss://silver@saasanalyticsdls.dfs.core.windows.net/subscriptions/')
df_source.display()

# COMMAND ----------

df_target=spark.read.format('delta').load('abfss://silver@saasanalyticsdls.dfs.core.windows.net/subscriptions_scd2')
join_df=df_source.alias('src').join(df_target.alias('tgt'),on=['subscription_id'],how='left_outer')\
    .select(col('src.*'),
            col('tgt.subscription_id').alias('tgt_subscription_id'),
            col('tgt.customer_id').alias('tgt_customer_id'),
            col('tgt.plan_id').alias('tgt_plan_id'),
            col('tgt.status').alias('tgt_status'),
            col('tgt.sub_start_date').alias('tgt_sub_start_date'),
            col('tgt.sub_end_date').alias('tgt_sub_end_date'),
            col('tgt.last_updated').alias('tgt_last_updated'),
            col('tgt.ingestion_date').alias('tgt_ingestion_date'),
            col('tgt.source_file').alias('tgt_source_file'))

# COMMAND ----------

filter_df=join_df.filter(xxhash64(col('subscription_id'),col('customer_id'),col('plan_id'),col('status'),col('sub_start_date'),col('sub_end_date'),col('last_updated'),col('ingestion_date'),col('source_file'))!=
                         xxhash64(col('tgt_subscription_id'),col('tgt_customer_id'),col('tgt_plan_id'),col('tgt_status'),col('tgt_sub_start_date'),col('tgt_sub_end_date'),col('tgt_last_updated'),col('tgt_ingestion_date'),col('tgt_source_file')))

# COMMAND ----------

merge_df=filter_df.withColumn('MERGEKEY',concat(col('subscription_id')))
dummy_df=filter_df.filter('tgt_subscription_id is not null').withColumn('MERGEKEY',lit(None))
scd_df=merge_df.union(dummy_df)
scd_df.orderBy(col('subscription_id')).display()

# COMMAND ----------

target_df= DeltaTable.forName(spark, "delta_catalog.silver.subscriptions_scd2")

target_df.alias('tgt').merge(
    scd_df.alias('src'),
    'tgt.subscription_id=src.MERGEKEY and tgt.active_status="Y"'
).whenMatchedUpdate(set={
    'active_status':lit('N'),
    'end_date':current_timestamp()
}).whenNotMatchedInsert(values={
    'subscription_id':'src.subscription_id',
    'customer_id':'src.customer_id',
    'plan_id':'src.plan_id',
    'status':'src.status',
    'sub_start_date':'src.sub_start_date',
    'sub_end_date':'src.sub_end_date',
    'last_updated':'src.last_updated',
    'ingestion_date':'src.ingestion_date',
    'source_file':'src.source_file',
    'start_date':current_timestamp(),
    'active_status':lit('Y')
}).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.silver.subscriptions_scd2 order by subscription_id

# COMMAND ----------

df=spark.read.format('delta').load('abfss://silver@saasanalyticsdls.dfs.core.windows.net/subscriptions_scd2')
df.orderBy(col('subscription_id')).display()

# COMMAND ----------

