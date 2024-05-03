-- CTRL-SHIFT-ENTER to run a selected fragment

--- create base views
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_job_runs_tasks as SELECT r.* FROM akrinsky_dbsql_logging.finops.job_runs_tasks r;
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_job_runs as SELECT r.* FROM akrinsky_dbsql_logging.finops.job_runs r;
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_jobs as SELECT j.* FROM akrinsky_dbsql_logging.finops.jobs j;
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_instance_pools as SELECT p.* FROM akrinsky_dbsql_logging.finops.instance_pools p;
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_dlt_pipelines as SELECT d.* FROM akrinsky_dbsql_logging.finops.dlt_pipelines d;
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_clusters as SELECT c.* FROM akrinsky_dbsql_logging.finops.clusters c;

-- v_system_usage_cost (start here)
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_system_usage_cost AS
SELECT usage.custom_tags, usage.usage_metadata, usage.usage_quantity, list_prices.pricing["default"] est_dbu_cost, usage_date, usage.sku_name, workspace_id
FROM system.billing.usage usage
    INNER JOIN system.billing.list_prices list_prices on
      usage.cloud = list_prices.cloud and
      usage.sku_name = list_prices.sku_name and
      usage.usage_start_time >= list_prices.price_start_time and
      (usage.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null);



