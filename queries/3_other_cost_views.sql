CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_shared_cluster_job_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT 
    jr.job_name, 
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    cl.cluster_name,
    cl.access_mode cluster_access_mode,
    cl.cluster_source,
    c.usage_date,
    SUM(jrt.execution_duration) AS day_cluster_creator_task_exec_duration
    FROM akrinsky_dbsql_logging.finops.v_system_usage_cost c 
    -- Filtering on clusters that have a billing record
    INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date)
    INNER JOIN akrinsky_dbsql_logging.finops.v_clusters cl
      on (jrt.cluster_id=cl.cluster_id)
    INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    --INNER JOIN akrinsky_dbsql_logging.finops.v_jobs j
    --  on (jr.job_id=j.job_id) 
    INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    -- Filtering on billing record in last 30 days
    WHERE c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY jr.job_name, jrt.cluster_id, cl.cluster_name,c.usage_date, w.workspace_id, 
    w.workspace_name, cl.access_mode,cl.cluster_source;


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






