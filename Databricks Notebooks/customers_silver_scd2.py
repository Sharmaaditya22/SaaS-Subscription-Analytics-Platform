# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating target Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.silver.customers_scd2
# MAGIC using delta
# MAGIC location 'abfss://silver@saasanalyticsdls.dfs.core.windows.net/customers_scd2'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'false'
# MAGIC )
# MAGIC as
# MAGIC select * from delta_catalog.silver.customers limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta_catalog.silver.customers_scd2;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta_catalog.silver.customers_scd2
# MAGIC ADD COLUMNS (
# MAGIC     start_date TIMESTAMP,
# MAGIC     end_date TIMESTAMP,
# MAGIC     active_status STRING
# MAGIC );
# MAGIC update delta_catalog.silver.customers_scd2 set start_date=current_timestamp(),active_status='Y'

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta_catalog.silver.customers_scd2 set country='test',is_valid_country='false' where country='IN'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.silver.customers_scd2

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD 2 Logic

# COMMAND ----------

df_target=spark.read.format('delta').load('abfss://silver@saasanalyticsdls.dfs.core.windows.net/customers_scd2')
join_df=df_source.alias('src').join(df_target.alias('tgt'), on='customer_id', how='left_outer')\
    .select(col('src.*'),
            col('tgt.customer_id').alias('tgt_customer_id'),
            col('tgt.email_hash').alias('tgt_email_hash'),
            col('tgt.country').alias('tgt_country'),
            col('tgt.created_at').alias('tgt_created_at'),
            col('tgt.last_updated').alias('tgt_last_updated'),
            col('tgt.ingestion_date').alias('tgt_ingestion_date'),
            col('tgt.source_file').alias('tgt_source_file'),
            col('tgt.is_valid_country').alias('tgt_is_valid_country'))

# COMMAND ----------

filter_df=join_df.filter(xxhash64(col('customer_id'),col('email_hash'),col('country'),col('created_at'),col('last_updated'),col('ingestion_date'),col('source_file'),col('is_valid_country'))!=
                         xxhash64(col('tgt_customer_id'),col('tgt_email_hash'),col('tgt_country'),col('tgt_created_at'),col('tgt_last_updated'),col('tgt_ingestion_date'),col('tgt_source_file'),col('tgt_is_valid_country')))

# COMMAND ----------

merge_df=filter_df.withColumn('MERGEKEY',concat(col('customer_id')))
dummy_df=filter_df.filter('tgt_customer_id is not null')\
.withColumn('MERGEKEY',lit(None))
scd_df=merge_df.union(dummy_df)

# COMMAND ----------

target_df= DeltaTable.forName(spark, "delta_catalog.silver.customers_scd2")

target_df.alias('tgt').merge(
    scd_df.alias('src'),
    'tgt.customer_id = src.MERGEKEY and tgt.active_status="Y"'
).whenMatchedUpdate(set={
  'active_status':lit('N'),
  'end_date':current_timestamp()
}).whenNotMatchedInsert(values={
  'customer_id':'src.customer_id',
  'email_hash':'src.email_hash',
  'country':'src.country',
  'created_at':'src.created_at',
  'last_updated':'src.last_updated',
  'ingestion_date':'src.ingestion_date',
  'source_file':'src.source_file',
  'is_valid_country':'src.is_valid_country',
  'start_date':current_timestamp(),
  'active_status':lit('Y')
}).execute()

# COMMAND ----------

df=spark.read.format('delta').load('abfss://silver@saasanalyticsdls.dfs.core.windows.net/customers_scd2')
df.orderBy(col('customer_id')).display()

# COMMAND ----------

