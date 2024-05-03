-- apportioned cost by jobs running on shared clusters
-- For jobs that run on shared job clusters

-- intermediary query 1 (each creator)
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_shared_cluster_creator_job_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT 
    jr.creator_user_name AS job_run_creator, 
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    c.usage_date,
    SUM(jrt.execution_duration) AS day_cluster_creator_task_exec_duration
    FROM akrinsky_dbsql_logging.finops.v_system_usage_cost c 
    -- Filtering on clusters that have a billing record
    INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date)
    INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    --INNER JOIN akrinsky_dbsql_logging.finops.v_jobs j
    --  on (jr.job_id=j.job_id) 
    INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    -- Filtering on billing record in last 30 days
    WHERE c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY jr.creator_user_name, jrt.cluster_id,c.usage_date, w.workspace_id, 
    w.workspace_name

-- SELECT * FROM akrinsky_dbsql_logging.finops.v_shared_cluster_job_cost;

-- intermediary query 2 (costs and duration across all creators)
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_shared_cluster_job_duration_cost AS 
    SELECT 
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    c.usage_date,
    -- all tasks on this clsuter, duration, each day
    SUM(jrt.execution_duration) AS day_cluster_task_exec_duration,
    -- total costs for this cluster, each day
    SUM(c.est_dbu_cost) AS day_cluster_est_dbu_cost
    FROM akrinsky_dbsql_logging.finops.v_system_usage_cost c 
    INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date)
    INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    --INNER JOIN akrinsky_dbsql_logging.finops.v_jobs j
    --  on (jrt.job_id=j.job_id) 
    INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    WHERE c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY jrt.cluster_id,c.usage_date, w.workspace_id, 
    w.workspace_name

 
-- intermediary query 3: weighted cost of creator jobs on each cluster, as well as % cluster used (assumes only tasks run on clusters)
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_shared_cluster_creator_job_duration_weighted_cost AS 
SELECT creator_duration.job_run_creator, 
creator_duration.usage_date,
creator_duration.cluster_id,
-- (sum of creator task duration) / (total of task duration on cluster)
sum(creator_duration.day_cluster_creator_task_exec_duration) creator_exec_duration,
min(cluster_cost_duration.day_cluster_task_exec_duration) cluster_exec_duration,
creator_exec_duration/cluster_exec_duration as exec_duration_cluster_pct,
min(cluster_cost_duration.day_cluster_est_dbu_cost)*exec_duration_cluster_pct as exec_duration_weighted_cluster_cost
FROM akrinsky_dbsql_logging.finops.v_shared_cluster_creator_job_duration creator_duration
INNER JOIN akrinsky_dbsql_logging.finops.v_shared_cluster_job_duration_cost AS cluster_cost_duration
ON (creator_duration.cluster_id=cluster_cost_duration.cluster_id AND creator_duration.usage_date=cluster_cost_duration.usage_date )
GROUP BY creator_duration.job_run_creator, creator_duration.usage_date,creator_duration.cluster_id;

-- final apportionment
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_cost_byshared_cluster_apportionment AS 
SELECT job_run_creator,
sum(exec_duration_weighted_cluster_cost) creator_exec_duration_weighted_cluster_cost,
-- use min to avoid double counting
ROUND(creator_exec_duration_weighted_cluster_cost/min(tots.total_exec_duration_weighted_cluster_cost) ,4) as period_pct_cost
FROM akrinsky_dbsql_logging.finops.v_shared_cluster_creator_job_duration_weighted_cost creator_weighted_cost
-- cartesian
INNER JOIN (
  SELECT sum(exec_duration_weighted_cluster_cost) as total_exec_duration_weighted_cluster_cost
  FROM akrinsky_dbsql_logging.finops.v_shared_cluster_creator_job_duration_weighted_cost
) as tots
GROUP BY job_run_creator
ORDER BY sum(exec_duration_weighted_cluster_cost) DESC



-- Replace x_Owner with whatever tag you need
-- create breakdowns by a custom_tags.<tag> for prior 30 days
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_cost_bytag_apportionment AS 
SELECT custom_tags.x_Owner businessUnit, 
sum(est_dbu_cost) bu_cost, 
min(agg_cost.total_cost) total_cost,
ROUND(sum(est_dbu_cost)/min(agg_cost.total_cost),4) period_pct_cost
from akrinsky_dbsql_logging.finops.v_system_usage_cost, 
(
SELECT sum(est_dbu_cost) total_cost
from akrinsky_dbsql_logging.finops.v_system_usage_cost
WHERE  usage_date >= DATE_SUB(CURRENT_DATE(), 30)
) as agg_cost
WHERE  usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY custom_tags.x_Owner;


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
