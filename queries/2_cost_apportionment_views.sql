-- apportioned cost by jobs running on shared clusters
-- For jobs that run on shared job clusters

-- intermediary query 1
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_shared_cluster_job_cost AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT COUNT(*) CNT FROM (

    SELECT 
    jr.creator_user_name AS job_run_creator, 
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    c.usage_date,
    --j.name AS job_name,
    jr.result_state,
    SUM(c.est_dbu_cost) AS cluster_cost, 
    SUM(jrt.execution_duration) AS task_exec_duration
    FROM akrinsky_dbsql_logging.finops.v_system_usage_cost c 
    INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id)
    INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    --INNER JOIN akrinsky_dbsql_logging.finops.v_jobs j
    --  on (jrt.job_id=j.job_id) 
    INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    WHERE c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
    AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date 
    GROUP BY jr.creator_user_name, jrt.cluster_id,c.usage_date, w.workspace_id, 
    w.workspace_name,jr.result_state
    --,j.name

    )

-- intermediary query 2
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_shared_cluster_jobs_daily_totals AS 
    SELECT 
    c.usage_date,
    c.cluster_id,
    w.workspace_id, 
    w.workspace_name,
    SUM(c.task_exec_duration) AS cluster_exec_duration,
    MIN(c.cluster_cost) as cluster_cost
    FROM akrinsky_dbsql_logging.finops.v_shared_cluster_job_cost as c
   INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    GROUP BY c.usage_date,c.cluster_id,
    w.workspace_id, 
    w.workspace_name;
 
-- intermediary query 3
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_shared_cluster_weighted_cost AS 
SELECT scjc.job_run_creator, 
scjc.usage_date,
min(c.cluster_cost)*(sum(scjc.task_exec_duration)/min(c.cluster_exec_duration)) as exec_duration_weighted_cluster_cost
FROM akrinsky_dbsql_logging.finops.v_shared_cluster_job_cost scjc
INNER JOIN akrinsky_dbsql_logging.finops.v_shared_cluster_jobs_daily_totals AS c
ON (scjc.cluster_id=c.cluster_id AND scjc.usage_date=c.usage_date )
GROUP BY scjc.job_run_creator, scjc.usage_date;

-- intermediary query 4
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_cost_byshared_cluster_apportionment AS 
SELECT job_run_creator,
sum(exec_duration_weighted_cluster_cost) period_exec_duration_weighted_cluster_cost,
min(tots.total_exec_duration_weighted_cluster_cost) total_exec_duration_weighted_cluster_cost,
sum(exec_duration_weighted_cluster_cost)/min(tots.total_exec_duration_weighted_cluster_cost) as period_pct_cost
FROM akrinsky_dbsql_logging.finops.v_shared_cluster_weighted_cost
-- cartesian
INNER JOIN (
  SELECT sum(exec_duration_weighted_cluster_cost) as total_exec_duration_weighted_cluster_cost
  FROM akrinsky_dbsql_logging.finops.v_shared_cluster_weighted_cost
) as tots
GROUP BY job_run_creator
ORDER BY sum(exec_duration_weighted_cluster_cost) DESC



-- Replace x_Owner with whatever tag you need
-- create breakdowns by a custom_tags.<tag> for prior 30 days
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_cost_bytag_apportionment AS 
SELECT custom_tags.x_Owner businessUnit, 
sum(est_dbu_cost) bu_cost, 
min(agg_cost.total_cost) total_cost,
ROUND(sum(est_dbu_cost)/min(agg_cost.total_cost)) pct_total
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
SELECT j.creator_user_name AS job_runner, SUM(est_dbu_cost) AS job_cost, any_value(agg_job_cost.total_cost) AS all_job_cost
FROM akrinsky_dbsql_logging.finops.v_system_usage_cost
INNER JOIN akrinsky_dbsql_logging.finops.v_job_runs j 
ON j.run_id = usage_metadata["job_run_id"]
INNER JOIN 
  (
    SELECT SUM(c.est_dbu_cost) AS total_cost
    FROM akrinsky_dbsql_logging.finops.v_system_usage_cost c
    WHERE c.usage_metadata["job_run_id"] is not null
      AND c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS agg_job_cost
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY j.creator_user_name;