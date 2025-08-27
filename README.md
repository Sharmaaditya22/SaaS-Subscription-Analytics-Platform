# SaaS-Subscription-Analytics-Platform

Azure Data Factory · Azure Databricks · Azure Synapse · Power BI

📌 Project Overview

This project demonstrates how to build an end-to-end SaaS analytics data platform on Azure using the medallion architecture (Bronze → Silver → Gold).

The goal is to track subscription lifecycle, revenue KPIs, and customer retention. The solution integrates batch (incremental) and streaming (event) data sources into curated analytics tables ready for BI reporting.

🔧 Architecture

Source Systems

Batch: Azure SQL Database (customers, plans, subscriptions, invoices)

Streaming: Event data (e.g., trial_started, plan_upgraded, invoice_paid) landed in ADLS

Ingestion Layer (ADF)

ADF pipelines perform incremental (watermark) copies from Azure SQL into ADLS Bronze.

Event files land directly into ADLS landing folders.

Processing Layer (Databricks)

Bronze: Raw ingested data + metadata (no heavy transforms).

Silver: Cleaned, conformed, deduplicated data. Built SCD Type-2 dimensions for customers, plans, and subscriptions.

Gold: Business-ready marts:

gold_kpi_daily: daily subscription/revenue KPIs (MRR, churn, expansion, contraction, ARPU).

gold_retention_cohorts: cohort analysis of customer retention over time.

Serving Layer

Gold Delta tables are written back to ADLS.

Designed so external tables/views can be created in Synapse Serverless to expose them in T-SQL for BI/analyst use.

Power BI dashboards can connect either directly to Databricks SQL Warehouse or through Synapse.

🗂️ Data Model
Silver (conformed)

dim_customer_scd2 – history of customer attributes (country, valid_from/valid_to).

dim_plan_scd2 – history of plan attributes (tier, monthly_price).

dim_subscription_scd2 – subscription lifecycle states across time.

invoices_silver, events_silver – cleaned fact-like sources.

Gold (marts)

gold_kpi_daily

Grain: one row per day.

Columns: dt, mrr, new_mrr, expansion_mrr, contraction_mrr, churn_mrr, active_customers, arr, arpu.

Derived by comparing day-over-day changes in subscription snapshots and joining with plan prices.

gold_retention_cohorts

Grain: cohort (signup month/week) × retention period.

Columns: cohort_month, period, customers_retained, retention_pct.

Derived by tracking active customers or logins across cohorts.

🚀 How to Run

Deploy Azure SQL Database and load sample SaaS data (DDL + inserts provided).

Create Azure Data Factory pipelines:

Use a config-driven Copy pipeline to land data into ADLS Bronze.

Stream event JSON files into ADLS landing.

Run Databricks notebooks:

bronze_notebook → load raw data into Delta Bronze.

silver_notebook → clean data, build SCD2 dimensions + facts.

gold_notebook → produce gold_kpi_daily and gold_retention_cohorts.

(Optional) In Synapse, create external tables/views on Gold tables for Power BI consumption.

📊 BI Outputs

Executive KPIs Dashboard (Power BI)

MRR/ARR trend

Churn breakdown (new vs expansion vs contraction vs churn MRR)

Active customer count & ARPU

Retention Cohorts Heatmap

Signup month vs retention % at W1, M1, M3, etc.

⚡ Key Learnings

Built a config-driven ingestion pipeline in ADF (single Copy activity looping over config table in CSV).

Modeled SCD Type-2 dimensions for customers, plans, and subscriptions to track history.

Designed Gold marts to align with business KPIs for SaaS products.

Tables are stored in Delta Lake format so Synapse Serverless can query them via external tables.