# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze layer for dimension table

# COMMAND ----------

plan_df=spark.read.parquet('abfss://landing@saasanalyticsdls.dfs.core.windows.net/plans/')
plan_df=plan_df.withColumn('ingestion_date',current_timestamp())\
    .withColumn('source_file',lit('plans_file'))

plan_df.write\
    .format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .option('path','abfss://bronze@saasanalyticsdls.dfs.core.windows.net/plans')\
    .saveAsTable('delta_catalog.bronze.plans')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.bronze.plans

# COMMAND ----------

customers_df=spark.read.parquet('abfss://landing@saasanalyticsdls.dfs.core.windows.net/customers/')
customers_df=customers_df.withColumn('ingestion_date',current_timestamp())\
    .withColumn('source_file',lit('customer_file'))

customers_df.write\
    .format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .option('path','abfss://bronze@saasanalyticsdls.dfs.core.windows.net/customers')\
    .saveAsTable('delta_catalog.bronze.customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.bronze.customers

# COMMAND ----------

invoices_df=spark.read.parquet('abfss://landing@saasanalyticsdls.dfs.core.windows.net/invoices/')
invoices_df=invoices_df.withColumn('ingestion_date',current_timestamp())\
    .withColumn('source_file',lit('invoices_file'))

invoices_df.write\
    .format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .option('path','abfss://bronze@saasanalyticsdls.dfs.core.windows.net/invoices')\
    .saveAsTable('delta_catalog.bronze.invoices')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.bronze.invoices

# COMMAND ----------

subscriptions_df=spark.read.parquet('abfss://landing@saasanalyticsdls.dfs.core.windows.net/subscriptions/')
subscriptions_df=subscriptions_df.withColumn('ingestion_date',current_timestamp())\
    .withColumn('source_file',lit('subscriptions_file'))

subscriptions_df.write\
    .format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .option('path','abfss://bronze@saasanalyticsdls.dfs.core.windows.net/subscriptions')\
    .saveAsTable('delta_catalog.bronze.subscriptions')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.bronze.subscriptions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze layer for Fact table

# COMMAND ----------

events_df=spark.read.json('abfss://landing@saasanalyticsdls.dfs.core.windows.net/events/')
events_df=events_df.withColumn('customer_id',col('payload.customer_id'))\
    .withColumn('amount',col('payload.amount'))\
    .withColumn('failure_reason',col('payload.failure_reason'))\
    .drop(col('payload'))\
    .withColumn('ingestion_date',current_timestamp())\
    .withColumn('source_file',lit('events_file'))\


events_df.write\
    .format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .option('path','abfss://bronze@saasanalyticsdls.dfs.core.windows.net/events')\
    .saveAsTable('delta_catalog.bronze.events')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.bronze.events

# COMMAND ----------

