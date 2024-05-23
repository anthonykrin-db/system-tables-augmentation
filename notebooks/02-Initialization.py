# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

# DBTITLE 1,Get Url Credentials
URL_CREDS = get_api_endpoints()
for url, cred in URL_CREDS:
  auth_header = {"Authorization" : "Bearer " + cred}
  print("Workspace URL: {}, auth: {}".format(url,cred))

# COMMAND ----------

spark.sql(f'CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}') 
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME}') 
# optional: add location 
# spark.sql(f'CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} LOCATION {DATABASE_LOCATION}') 

# COMMAND ----------

# DBTITLE 1,Create workspaces
# WORKSPACE_NAMES

data = [(workspace_id, workspace_name) for workspace_id, workspace_name in WORKSPACE_NAMES.items()]
# Create the DataFrame
workspace_names_df = spark.createDataFrame(data, ["workspace_id", "workspace_name"])
workspace_names_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + WORKSPACE_TABLE_NAME)

print("Created workspaces")


# COMMAND ----------

# DBTITLE 1,create dbu discounts
from datetime import datetime, timedelta
from decimal import Decimal
import pandas as pd
from pyspark.sql.functions import current_timestamp,col, date_format

discounts_df = densify_monthly_config_df(DBU_DISCOUNTS,"discount")
# Save the DataFrame as a Delta table
discounts_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + DBU_DISCOUNT_TABLE_NAME)

print("Created dbu discounts")

# COMMAND ----------

# DBTITLE 1,create markups
# INFRA_MARKUPS

# INFRA_MARKUP_TABLE_NAME = SCHEMA_NAME+".infra_markups"
discounts_df = densify_monthly_config_df(INFRA_MARKUPS,"amount")
# Save the DataFrame as a Delta table
discounts_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + INFRA_MARKUP_TABLE_NAME)

print("Created markups")

