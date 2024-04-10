# Databricks notebook source
# DBTITLE 1,API Config
# Please ensure the url starts with https and DOES NOT have a slash at the end
# Lists all objects that a user has manager permissions on.# 
WORKSPACE_HOST = 'https://adb-984752964297111.11.azuredatabricks.net'
WAREHOUSES_URL = "{0}/api/2.0/sql/warehouses".format(WORKSPACE_HOST) ## SQL Warehouses APIs 2.1
JOBS_URL = "{0}/api/2.1/jobs/list".format(WORKSPACE_HOST) ## Jobs & Workflows History API 2.1 
CLUSTERS_URL = "{0}/api/2.0/clusters/list".format(WORKSPACE_HOST)
DASHBOARDS_URL = "{0}/api/2.0/preview/sql/dashboards".format(WORKSPACE_HOST) ## Queries and Dashboards API - ❗️in preview, deprecated soon❗️
WORKSPACE_OBJECTS_URL = "{0}/api/2.0/workspace/list".format(WORKSPACE_HOST)
INSTANCE_POOLS_URL = "{0}/api/2.0/instance-pools/list".format(WORKSPACE_HOST)
DLT_PIPELINES_URL = "{0}/api/2.0/pipelines".format(WORKSPACE_HOST)

MAX_RESULTS_PER_PAGE = 1000
MAX_PAGES_PER_RUN = 500
PAGE_SIZE = 250 # 250 is the max

# COMMAND ----------

# DBTITLE 1,API Authentication
# If you want to run this notebook yourself, you need to create a Databricks personal access token,
# store it using our secrets API, and pass it in through the Spark config, such as this:
# spark.pat_token {{secrets/query_history_etl/user}}, or Azure Keyvault.

#Databricks secrets API
#AUTH_HEADER = {"Authorization" : "Bearer " + spark.conf.get("spark.pat_token")}
#Azure KeyVault
#AUTH_HEADER = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")}
#Naughty way
AUTH_HEADER = {"Authorization" : "Bearer " + "your_token_here"}

# COMMAND ----------

# DBTITLE 1,Database and Table Config
DATABASE_NAME = "akrinsky_dbsql_logging"
SCHEMA_NAME = "tacklebox"
WAREHOUSES_TABLE_NAME = SCHEMA_NAME+".warehouses"
JOBS_TABLE_NAME = SCHEMA_NAME+".jobs"
DASHBOARDS_TABLE_NAME = SCHEMA_NAME+".dashboards_preview"
CLUSTERS_TABLE_NAME = SCHEMA_NAME+".clusters"
WORKSPACE_OBJECTS_TABLE_NAME = SCHEMA_NAME+".workspace_objects"
INSTANCE_POOLS_TABLE_NAME = SCHEMA_NAME+".instance_pools"
DLT_PIPELINES_TABLE_NAME = SCHEMA_NAME+".dlt_pipelines"

# COMMAND ----------

# DBTITLE 1,Delta Table Maintenance
QUERIES_ZORDER = "endpoint_id"
WAREHOUSES_ZORDER = "id"
WORKFLOWS_ZORDER = "job_id"
DASHBOARDS_ZORDER = "id"

VACUUM_RETENTION = 168
