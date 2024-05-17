# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

execute_multiline_sql_file("../queries/0_base_views.sql")
execute_multiline_sql_file("../queries/1_job_cost_apportionment_views.sql")
execute_multiline_sql_file("../queries/2_other_cost_views.sql")
execute_multiline_sql_file("../queries/3_usage_views.sql")

