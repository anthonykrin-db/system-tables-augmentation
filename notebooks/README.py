# Databricks notebook source
# MAGIC %md
# MAGIC #System table augmentation
# MAGIC
# MAGIC ## Permissions
# MAGIC
# MAGIC Note that this solution uses Databricks APIs to retrieve data. 
# MAGIC
# MAGIC ## Usage
# MAGIC <ol>
# MAGIC <li>Please set configuration found in in <b>00_Config</b></li>
# MAGIC <li>Create a Workflow that runds these notebooks in the following order, not more than once per hour:<br>
# MAGIC <ol><i>
# MAGIC <li>notebooks/03-ArchiveSystemTables</li>
# MAGIC <li> notebooks/04-ExtractDims-Core</li>
# MAGIC <li>notebooks/05-ExtractDims-Extras</li>
# MAGIC <li>notebooks/06-UpdateViews</li></i>
# MAGIC </ol>
# MAGIC </li>
# MAGIC
# MAGIC <li>If the schema specified in configuration is not empty, make sure that you can tolerate tables starting with <b>system_</b> being created there.</li>
# MAGIC <li>Download and install dashboards in /dashboards folder.</li>
# MAGIC </ol>
# MAGIC
# MAGIC ## Configuration
# MAGIC
# MAGIC Edit these entries in <b>00_Config</b>
# MAGIC <i>Note that if these configurations are changed, hard-coded references in views must be changed</i>
# MAGIC  ```
# MAGIC ################
# MAGIC # Critical configuration
# MAGIC  DATABASE_NAME = "finops"
# MAGIC SCHEMA_NAME = "system_lookups"
# MAGIC ################
# MAGIC # Workspace in which to read system tables and write observability schema, tables, and views
# MAGIC WORKSPACE_HOST = 'https://adb-984752964297111.11.azuredatabricks.net'
# MAGIC ################
# MAGIC # Service account token for API use
# MAGIC AUTH_HEADER = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")}
# MAGIC ################
# MAGIC # Configurable discount for each month, can skip
# MAGIC # Be sure to include a distant end date
# MAGIC DBU_DISCOUNTS = {
# MAGIC   "01-2020":"0.235",
# MAGIC   "01-2030":"0.235"
# MAGIC }
# MAGIC ################
# MAGIC # Configurable infrastructure markup for each month can skp
# MAGIC # Be sure to include a distant end date
# MAGIC # Typical markups are 75% of DBU including storage fees
# MAGIC INFRA_MARKUPS = {
# MAGIC   "01-2020":"0.65",
# MAGIC   "03-2024":"0.55",
# MAGIC   "01-2030":"0.55"
# MAGIC }
# MAGIC ################
# MAGIC # Workspace ID and name pairs.  Unfortunately, this is not yet exposedin system tables.
# MAGIC WORKSPACE_NAMES = {
# MAGIC "6024433575559853":"WKSP59853",
# MAGIC "6058900950367176":"WKSP67176",
# MAGIC "2574677666339144":"WKSP39144",
# MAGIC  ```
# MAGIC
# MAGIC
# MAGIC ## Backlog
# MAGIC
# MAGIC Check for gaps in case jobs don't finish but incremental field updates.
# MAGIC Remove hard coded references to schema/table names in views
# MAGIC
# MAGIC
