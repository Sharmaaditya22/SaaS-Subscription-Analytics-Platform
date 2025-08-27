# Databricks notebook source
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

VALID_ISO_CODES = {
    'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AQ', 'AR', 'AS', 'AT',
    'AU', 'AW', 'AX', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI',
    'BJ', 'BL', 'BM', 'BN', 'BO', 'BQ', 'BR', 'BS', 'BT', 'BV', 'BW', 'BY',
    'BZ', 'CA', 'CC', 'CD', 'CF', 'CG', 'CH', 'CI', 'CK', 'CL', 'CM', 'CN',
    'CO', 'CR', 'CU', 'CV', 'CW', 'CX', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM',
    'DO', 'DZ', 'EC', 'EE', 'EG', 'EH', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FK',
    'FM', 'FO', 'FR', 'GA', 'GB', 'GD', 'GE', 'GF', 'GG', 'GH', 'GI', 'GL',
    'GM', 'GN', 'GP', 'GQ', 'GR', 'GS', 'GT', 'GU', 'GW', 'GY', 'HK', 'HM',
    'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IO', 'IQ', 'IR',
    'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KM', 'KN',
    'KP', 'KR', 'KW', 'KY', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS',
    'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH', 'MK',
    'ML', 'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS', 'MT', 'MU', 'MV', 'MW',
    'MX', 'MY', 'MZ', 'NA', 'NC', 'NE', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP',
    'NR', 'NU', 'NZ', 'OM', 'PA', 'PE', 'PF', 'PG', 'PH', 'PK', 'PL', 'PM',
    'PN', 'PR', 'PS', 'PT', 'PW', 'PY', 'QA', 'RE', 'RO', 'RS', 'RU', 'RW',
    'SA', 'SB', 'SC', 'SD', 'SE', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM',
    'SN', 'SO', 'SR', 'SS', 'ST', 'SV', 'SX', 'SY', 'SZ', 'TC', 'TD', 'TF',
    'TG', 'TH', 'TJ', 'TK', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TV', 'TW',
    'TZ', 'UA', 'UG', 'UM', 'US', 'UY', 'UZ', 'VA', 'VC', 'VE', 'VG', 'VI',
    'VN', 'VU', 'WF', 'WS', 'YE', 'YT', 'ZA', 'ZM', 'ZW'
}

def validate_country_code(code):
    return code.upper() in VALID_ISO_CODES

validate_udf=udf(validate_country_code,BooleanType())

# COMMAND ----------

silver_customers_df=spark.read.format('delta').load('abfss://bronze@saasanalyticsdls.dfs.core.windows.net/customers/')

silver_customers_df=silver_customers_df.dropDuplicates(['customer_id'])

email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
silver_customers_df=silver_customers_df.filter(~(col('email_hash').rlike(email_regex)))

silver_customers_df=silver_customers_df.withColumn('is_valid_country',validate_udf(col('country')))\
    .filter(col('is_valid_country'))

silver_customers_df.write\
.format('delta')\
.mode('overwrite')\
.option('mergeSchema', 'true')\
.option('path','abfss://silver@saasanalyticsdls.dfs.core.windows.net/customers')\
.saveAsTable('delta_catalog.silver.customers')


# COMMAND ----------

silver_plans_df=spark.read.format('delta').load('abfss://bronze@saasanalyticsdls.dfs.core.windows.net/plans/')

silver_plans_df=silver_plans_df.dropDuplicates(['plan_id','effective_from'])

silver_plans_df.write\
.format('delta')\
.mode('overwrite')\
.option('mergeSchema', 'true')\
.option('path','abfss://silver@saasanalyticsdls.dfs.core.windows.net/plans')\
.saveAsTable('delta_catalog.silver.plans')

# COMMAND ----------

silver_subscriptions_df=spark.read.format('delta').load('abfss://bronze@saasanalyticsdls.dfs.core.windows.net/subscriptions/')

silver_subscriptions_df=silver_subscriptions_df.dropDuplicates(['subscription_id','start_date'])

silver_subscriptions_df=silver_subscriptions_df.filter(~(col('end_date').isNull() & (col('status')=='canceled')) | ~(col('end_date').isNotNull() & (col('status')=='active')))\
    .select(col('subscription_id'),col('customer_id'),col('plan_id'),col('status'),col('start_date').alias('sub_start_date'),col('end_date').alias('sub_end_date'),col('last_updated'),col('ingestion_date'),col('source_file'))


silver_subscriptions_df.write\
.format('delta')\
.mode('overwrite')\
.option('mergeSchema', 'true')\
.option('path','abfss://silver@saasanalyticsdls.dfs.core.windows.net/subscriptions')\
.saveAsTable('delta_catalog.silver.subscriptions')

# COMMAND ----------

silver_invoices_df=spark.read.format('delta').load('abfss://bronze@saasanalyticsdls.dfs.core.windows.net/invoices/')

silver_invoices_df=silver_invoices_df.dropDuplicates(['invoice_id'])

silver_invoices_df.filter(((col('amount')>=0) & (col('currency')=='USD')) | ((col('paid')=='false') & col('failure_reason').isNotNull()))

silver_invoices_df.write\
.format('delta')\
.mode('overwrite')\
.option('mergeSchema', 'true')\
.option('path','abfss://silver@saasanalyticsdls.dfs.core.windows.net/invoices')\
.saveAsTable('delta_catalog.silver.invoices')

# COMMAND ----------

silver_events_df=spark.read.format('delta').load('abfss://bronze@saasanalyticsdls.dfs.core.windows.net/events/')

silver_events_df=silver_events_df.dropDuplicates(['event_id'])

silver_events_df=silver_events_df.withColumn('event_ts',to_timestamp(col('event_ts'),'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))

silver_events_df.write\
.format('delta')\
.mode('overwrite')\
.option('mergeSchema', 'true')\
.option('path','abfss://silver@saasanalyticsdls.dfs.core.windows.net/events')\
.saveAsTable('delta_catalog.silver.events')

# COMMAND ----------

