# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating target Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.silver.plans

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.silver.plans_scd2
# MAGIC using delta
# MAGIC location 'abfss://silver@saasanalyticsdls.dfs.core.windows.net/plans_scd2'
# MAGIC as
# MAGIC select * from delta_catalog.silver.plans where plan_name in ('Basic','Pro')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta_catalog.silver.plans_scd2
# MAGIC ADD COLUMNS (
# MAGIC     start_date TIMESTAMP,
# MAGIC     end_date TIMESTAMP,
# MAGIC     active_status STRING
# MAGIC );
# MAGIC update delta_catalog.silver.plans_scd2 set start_date=current_timestamp(),active_status='Y'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.silver.plans_scd2

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD 2 Logic

# COMMAND ----------

df_plan_source=spark.read.format('delta').load('abfss://silver@saasanalyticsdls.dfs.core.windows.net/plans')

# COMMAND ----------

df_plan_source=df_plan_source.withColumn('monthly_price',when(col('plan_name')=='Basic',lit(101)).otherwise(col('monthly_price')))

# COMMAND ----------

df_plan_source.display()

# COMMAND ----------

df_plan_target=spark.read.format('delta').load('abfss://silver@saasanalyticsdls.dfs.core.windows.net/plans_scd2')
join_df=df_plan_source.alias('src').join(df_plan_target.alias('tgt'), col('src.plan_id')==col('tgt.plan_id'), 'left_outer')\
    .select(col('src.*'),
            col('tgt.plan_id').alias('tgt_plan_id'),
            col('tgt.plan_name').alias('tgt_plan_name'),
            col('tgt.monthly_price').alias('tgt_monthly_price'),
            col('tgt.effective_from').alias('tgt_effective_from'),
            col('tgt.effective_to').alias('tgt_effective_to'),
            col('tgt.last_updated').alias('tgt_last_updated'),
            col('tgt.ingestion_date').alias('tgt_ingestion_date'),
            col('tgt.source_file').alias('tgt_source_file'))

# COMMAND ----------

filter_df=join_df.filter(xxhash64(col('plan_id'),col('plan_name'),col('monthly_price'),col('effective_from'),col('effective_to'),col('last_updated'),col('ingestion_date'),col('source_file'))!=
               xxhash64(col('tgt_plan_id'),col('tgt_plan_name'),col('tgt_monthly_price'),col('tgt_effective_from'),col('tgt_effective_to'),col('tgt_last_updated'),col('tgt_ingestion_date'),col('tgt_source_file')))

# COMMAND ----------

merge_df=filter_df.withColumn('MERGEKEY',concat(col('plan_id')))

# COMMAND ----------

dummy_df=filter_df.filter('tgt_plan_id is not null').withColumn('MERGEKEY',lit(None))
scd_df=merge_df.union(dummy_df)
scd_df.display()

# COMMAND ----------

df_plan_target = DeltaTable.forName(spark, "delta_catalog.silver.plans_scd2")

df_plan_target.alias('tgt').merge(
    scd_df.alias('src'),
    "concat(tgt.plan_id)=src.MERGEKEY and tgt.active_status='Y'"
).whenMatchedUpdate(set=
    {
    'active_status':lit('N'),
    'end_date':current_timestamp()
}
    ).whenNotMatchedInsert(values={
    'plan_id':'src.plan_id',
    'plan_name':'src.plan_name',
    'monthly_price':'src.monthly_price',
    'effective_from':'src.effective_from',
    'effective_to':'src.effective_to',
    'last_updated':'src.last_updated',
    'ingestion_date':'src.ingestion_date',
    'source_file':'src.source_file',
    'active_status':lit('Y'),
    'start_date':current_timestamp()
}).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.silver.plans_scd2

# COMMAND ----------

df_plan_target=spark.read.format('delta').load('abfss://silver@saasanalyticsdls.dfs.core.windows.net/plans_scd2')
df_plan_target.display()

# COMMAND ----------

