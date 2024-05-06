-- CTRL-SHIFT-ENTER to run a selected fragment

-- migrate data somewhere
-- CREATE TABLE  finops.system_lookups_dims.jobs AS SELECT * FROM akrinsky_dbsql_logging.finops.jobs;
-- CREATE TABLE  finops.system_lookups_dims.job_runs AS SELECT * FROM akrinsky_dbsql_logging.finops.job_runs;
-- CREATE TABLE  finops.system_lookups_dims.job_runs_tasks AS SELECT * FROM akrinsky_dbsql_logging.finops.job_runs_tasks;
-- CREATE TABLE  finops.system_lookups_dims.instance_pools AS SELECT * FROM akrinsky_dbsql_logging.finops.instance_pools;
-- CREATE TABLE  finops.system_lookups_dims.clusters AS SELECT * FROM akrinsky_dbsql_logging.finops.clusters;
-- CREATE TABLE  finops.system_lookups_dims.dlt_pipelines AS SELECT * FROM akrinsky_dbsql_logging.finops.dlt_pipelines;
-- CREATE TABLE  finops.system_lookups_dims.dashboards_preview AS SELECT * FROM akrinsky_dbsql_logging.finops.dashboards_preview;
-- CREATE TABLE  finops.system_lookups_dims.workspace_objects AS SELECT * FROM akrinsky_dbsql_logging.finops.workspace_objects;
-- CREATE TABLE  finops.system_lookups_dims.workspaces AS SELECT * FROM akrinsky_dbsql_logging.finops.workspaces;


--- create base views

DROP VIEW  finops.system_lookups_dims.v_job_runs_tasks;
DROP VIEW  finops.system_lookups_dims.v_job_runs;
DROP VIEW  finops.system_lookups_dims.v_jobs;
DROP VIEW  finops.system_lookups_dims.v_instance_pools;
DROP VIEW  finops.system_lookups_dims.v_dlt_pipelines;
DROP VIEW  finops.system_lookups_dims.v_clusters;
DROP VIEW  finops.system_lookups_dims.v_dashboards_preview;

CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_job_runs_tasks as 
SELECT jrt.*, j.name as job_name
FROM finops.system_lookups_dims.job_runs_tasks jrt INNER JOIN finops.system_lookups_dims.jobs j ON (j.job_id=jrt.job_id);

CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_job_runs as 
SELECT r.*,j.name as job_name FROM finops.system_lookups_dims.job_runs r INNER JOIN finops.system_lookups_dims.jobs j ON (j.job_id=r.job_id);

CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_jobs as SELECT j.* FROM finops.system_lookups_dims.jobs j;

CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_instance_pools as SELECT p.* FROM finops.system_lookups_dims.instance_pools p;

CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_dlt_pipelines as SELECT d.* FROM finops.system_lookups_dims.dlt_pipelines d;

CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_clusters as SELECT coalesce(access_mode,data_security_mode) access_mode, c.* EXCEPT(access_mode,data_security_mode) FROM finops.system_lookups_dims.clusters c;

CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_dashboards_preview as SELECT c.* FROM finops.system_lookups_dims.dashboards_preview c;

-- v_system_usage_cost (start here)
CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_system_usage_cost AS
SELECT usage.custom_tags, usage.usage_metadata, usage.usage_quantity, list_prices.pricing["default"] est_dbu_cost, usage_date, usage.sku_name, workspace_id
FROM system.billing.usage usage
    INNER JOIN system.billing.list_prices list_prices on
      usage.cloud = list_prices.cloud and
      usage.sku_name = list_prices.sku_name and
      usage.usage_start_time >= list_prices.price_start_time and
      (usage.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null);
