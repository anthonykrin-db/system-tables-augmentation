-- v_system_usage_cost (start here)
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_system_usage_cost AS
SELECT usage.custom_tags, usage.usage_metadata, usage.usage_quantity, list_prices.pricing["default"] list_cost, usage_date, usage.sku_name, workspace_id
FROM system.billing.usage usage
    INNER JOIN system.billing.list_prices list_prices on
      usage.cloud = list_prices.cloud and
      usage.sku_name = list_prices.sku_name and
      usage.usage_start_time >= list_prices.price_start_time and
      (usage.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null);


-- cost by job-run
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_cost_byjobrunner_apportionment AS 
SELECT j.creator_user_name AS job_runner, SUM(list_cost) AS job_cost, MIN(agg_job_cost.total_cost) AS all_job_cost
FROM akrinsky_dbsql_logging.finops.v_system_usage_cost
INNER JOIN akrinsky_dbsql_logging.finops.job_runs j 
ON j.run_id = usage_metadata["job_run_id"]
INNER JOIN 
  (
    SELECT SUM(c.list_cost) AS total_cost
    FROM akrinsky_dbsql_logging.finops.v_system_usage_cost c
    WHERE c.usage_metadata["job_run_id"] is not null
      AND c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS agg_job_cost
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY j.creator_user_name;

-- Replace x_Owner with whatever tag you need
-- create breakdowns by a custom_tags.<tag> for prior 30 days
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_cost_bytag_apportionment AS 
SELECT custom_tags.x_Owner businessUnit, sum(list_cost) bu_cost, min(agg_cost.total_cost) total_cost
--, ROUND(tag_cost/total_cost) pct_total
from akrinsky_dbsql_logging.finops.v_system_usage_cost, 
(
SELECT sum(list_cost) total_cost
from akrinsky_dbsql_logging.finops.v_system_usage_cost
WHERE  usage_date >= DATE_SUB(CURRENT_DATE(), 30)
) as agg_cost
WHERE  usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY custom_tags.x_Owner;

CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_job_runs as SELECT r.* FROM akrinsky_dbsql_logging.finops.job_runs r;
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_jobs as SELECT j.* FROM akrinsky_dbsql_logging.finops.jobs j;
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_instance_pools as SELECT p.* FROM akrinsky_dbsql_logging.finops.instance_pools p;
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_dlt_pipelines as SELECT d.* FROM akrinsky_dbsql_logging.finops.dlt_pipelines d;
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_clusters as SELECT c.* FROM akrinsky_dbsql_logging.finops.clusters c;