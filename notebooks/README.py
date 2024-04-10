# Databricks notebook source
# MAGIC %md
# MAGIC #System table augmentation
# MAGIC
# MAGIC ## Permissions
# MAGIC
# MAGIC Note that this solution uses Databricks APIs to retrieve data. 
# MAGIC
# MAGIC
# MAGIC ## Configuration
# MAGIC
# MAGIC Edit these entries in <b>00_Config</b>
# MAGIC
# MAGIC  ```
# MAGIC WORKSPACE_HOST = 'https://adb-984752964297111.11.azuredatabricks.net'
# MAGIC ...
# MAGIC AUTH_HEADER = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")}
# MAGIC ...
# MAGIC DATABASE_NAME = "akrinsky_dbsql_logging"
# MAGIC SCHEMA_NAME = "tacklebox"
# MAGIC
# MAGIC  ```
# MAGIC
# MAGIC ## Queries
# MAGIC  ```
# MAGIC
# MAGIC -- job_id
# MAGIC SELECT job_id,name FROM akrinsky_dbsql_logging.tacklebox.jobs
# MAGIC
# MAGIC -- cluster_id
# MAGIC SELECT cluster_id, cluster_name from akrinsky_dbsql_logging.tacklebox.clusters
# MAGIC
# MAGIC -- warehouse_id
# MAGIC SELECT id as warehouse_id, name from akrinsky_dbsql_logging.tacklebox.warehouses
# MAGIC
# MAGIC -- workspace object
# MAGIC SELECT object_id, path from akrinsky_dbsql_logging.tacklebox.workspace_objects
# MAGIC
# MAGIC -- dashboards preview
# MAGIC SELECT id, name from akrinsky_dbsql_logging.tacklebox.dashboards_preview
# MAGIC
# MAGIC -- instance pool
# MAGIC SELECT instance_pool_id, instance_pool_name from akrinsky_dbsql_logging.tacklebox.instance_pools
# MAGIC
# MAGIC
# MAGIC SELECT usage_date, sku_name, sum(usage_quantity) USAGE_QTY, custom_tags["Owner"] as Owner from system.billing.usage
# MAGIC GROUP BY  custom_tags["Owner"] , sku_name, usage_date
# MAGIC
# MAGIC SELECT * FROM system.billing.usage 
# MAGIC WHERE usage_metadata["job_id"]="abcdefg"
# MAGIC
# MAGIC SELECT usage_date, jobs.name, sum(usage.usage_quantity*list_prices.pricing["default"]) cost
# MAGIC FROM system.billing.usage usage
# MAGIC INNER JOIN system.billing.list_prices list_prices on
# MAGIC usage.cloud = list_prices.cloud and
# MAGIC usage.sku_name = list_prices.sku_name and
# MAGIC usage.usage_start_time >= list_prices.price_start_time and
# MAGIC (usage.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null)
# MAGIC INNER join akrinsky_dbsql_logging.tacklebox.jobs jobs on
# MAGIC jobs.job_id = usage.usage_metadata["job_id"]
# MAGIC GROUP BY
# MAGIC usage_date, jobs.name
# MAGIC
# MAGIC  ```
# MAGIC
# MAGIC ## Reset queries
# MAGIC
# MAGIC  ```
# MAGIC -- job_id
# MAGIC DROP TABLE IF EXISTS akrinsky_dbsql_logging.tacklebox.jobs;
# MAGIC
# MAGIC -- cluster_id
# MAGIC DROP TABLE  IF EXISTS  akrinsky_dbsql_logging.tacklebox.clusters;
# MAGIC
# MAGIC -- warehouse_id
# MAGIC DROP TABLE  IF EXISTS akrinsky_dbsql_logging.tacklebox.warehouses;
# MAGIC
# MAGIC -- workspace object
# MAGIC DROP  TABLE  IF EXISTS akrinsky_dbsql_logging.tacklebox.workspace_objects;
# MAGIC
# MAGIC -- dashboards preview
# MAGIC DROP  TABLE  IF EXISTS akrinsky_dbsql_logging.tacklebox.dashboards_preview;
# MAGIC
# MAGIC -- instance pool
# MAGIC DROP  TABLE  IF EXISTS akrinsky_dbsql_logging.tacklebox.instance_pools;
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC
