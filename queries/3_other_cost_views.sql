
-- cost by job-run
-- For jobs that run on their own job clusters
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_cost_byjob_cluster_apportionment AS 
SELECT j.creator_user_name AS job_runner, 
min(agg_cost.total_cost) total_cost,
ROUND(sum(est_dbu_cost)/min(agg_cost.total_cost),4) period_pct_cost
FROM akrinsky_dbsql_logging.finops.v_system_usage_cost
INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs j 
ON j.run_id = usage_metadata["job_run_id"]
INNER JOIN 
  (
    SELECT SUM(c.est_dbu_cost) AS total_cost
    FROM akrinsky_dbsql_logging.finops.v_system_usage_cost c
    WHERE c.usage_metadata["job_run_id"] is not null
      AND c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS agg_cost
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY j.creator_user_name;

-- v_cost_byworkspace
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_cost_byworkspace AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, sum(c.est_dbu_cost) est_dbu_cost
FROM  akrinsky_dbsql_logging.finops.v_system_usage_cost c
INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w ON (c.workspace_id=w.workspace_id)
GROUP BY c.workspace_id, w.workspace_name, c.usage_date, c.sku_name;

-- v_cost_bycluster
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_cost_bycluster AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, sum(c.est_dbu_cost) est_dbu_cost, cl.cluster_id, cl.cluster_name
FROM  akrinsky_dbsql_logging.finops.v_system_usage_cost c
INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w ON (c.workspace_id=w.workspace_id)
INNER JOIN akrinsky_dbsql_logging.finops.v_clusters cl ON (cl.cluster_id=c.usage_metadata["cluster_id"])
GROUP BY c.workspace_id, w.workspace_name, c. usage_date, c.sku_name,cl.cluster_id, cl.cluster_name;






