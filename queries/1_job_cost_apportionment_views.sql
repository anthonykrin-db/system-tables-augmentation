

CREATE OR REPLACE VIEW finops.system_lookups.v_job_run_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT 
    jrt.run_id,
    jr.job_name, 
    jr.job_id,
    jr.result_state,
    jr.state_message,
    sum(IF(jrt.attempt_number>0,1,0)) task_retries,
    if(sum(IF(jrt.attempt_number>0,1,0))>0,"RETRY","INIITAL") attempt_type,
    jr.creator_user_name AS job_run_creator, 
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    cl.cluster_name,
    cl.data_security_mode cluster_data_security_mode,
    cl.cluster_source,
    c.usage_date,
    SUM(jrt.execution_duration) AS job_run_exec_duration
    FROM finops.system_lookups.v_system_usage_cost c 
    -- Filtering on clusters that have a billing record
    INNER JOIN finops.system_lookups.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date)
    INNER JOIN finops.system_lookups.v_clusters cl
      on (jrt.cluster_id=cl.cluster_id)
    INNER JOIN finops.system_lookups.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    INNER JOIN finops.system_lookups.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    GROUP BY ALL;

CREATE OR REPLACE VIEW finops.system_lookups.v_job_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT * EXCEPT (job_run_exec_duration,run_id),
    SUM(c.job_run_exec_duration) AS job_runs_exec_duration
    FROM finops.system_lookups.v_job_run_duration c 
    GROUP BY ALL;

-- apportioned cost by jobs running on shared clusters
-- For jobs that run on shared job clusters
-- intermediary query 1 (each creator)
CREATE OR REPLACE VIEW finops.system_lookups.v_job_creator_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT * EXCEPT (job_run_exec_duration,run_id,job_id,job_name),
    SUM(c.job_run_exec_duration) AS job_creator_exec_duration
    FROM finops.system_lookups.v_job_run_duration c 
    GROUP BY ALL;

-- SELECT * FROM finops.system_lookups.v_shared_cluster_job_cost;

-- intermediary query 2 (costs and duration across all creators)
CREATE OR REPLACE VIEW finops.system_lookups.v_cluster_cost AS 
    SELECT 
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    cl.cluster_name,
    cl.data_security_mode cluster_data_security_mode,
    cl.cluster_source,
    c.usage_date,
    -- all tasks on this clsuter, duration, each day
    SUM(jrt.execution_duration) AS day_cluster_task_exec_duration,
    -- total costs for this cluster, each day
    SUM(c.est_dbu_cost) AS day_cluster_est_dbu_cost,
    SUM(c.est_total_cost) AS day_cluster_est_total_cost
    FROM finops.system_lookups.v_system_usage_cost c 
    INNER JOIN finops.system_lookups.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date)
    INNER JOIN finops.system_lookups.v_clusters cl
      on (jrt.cluster_id=cl.cluster_id)
    INNER JOIN finops.system_lookups.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    INNER JOIN finops.system_lookups.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    GROUP BY jrt.cluster_id,c.usage_date, w.workspace_id, cl.cluster_name, w.workspace_name, cl.data_security_mode,cl.cluster_source;


CREATE OR REPLACE VIEW  finops.system_lookups.v_job_weighted_cost AS 
SELECT 
job_duration.*,
round(sum(job_duration.job_runs_exec_duration) / 1000 /60,1) cluster_day_exec_duration_mins,
round(min(cluster_cost_duration.day_cluster_est_total_cost)*(sum(job_duration.job_runs_exec_duration)/min(cluster_cost_duration.day_cluster_task_exec_duration)),4) as exec_duration_weighted_cluster_cost
FROM finops.system_lookups.v_job_duration job_duration
INNER JOIN finops.system_lookups.v_cluster_cost AS cluster_cost_duration
ON (job_duration.cluster_id=cluster_cost_duration.cluster_id AND job_duration.usage_date=cluster_cost_duration.usage_date )
GROUP BY ALL;

CREATE OR REPLACE VIEW  finops.system_lookups.v_job_run_weighted_cost AS 
SELECT 
job_run_duration.*,
round(sum(job_run_duration.job_run_exec_duration) / 1000 /60,1) cluster_day_exec_duration_mins,
round(min(cluster_cost_duration.day_cluster_est_total_cost)*(sum(job_run_duration.job_run_exec_duration)/min(cluster_cost_duration.day_cluster_task_exec_duration)),4) as exec_duration_weighted_cluster_cost
FROM finops.system_lookups.v_job_run_duration job_run_duration
INNER JOIN finops.system_lookups.v_cluster_cost AS cluster_cost_duration
ON (job_run_duration.cluster_id=cluster_cost_duration.cluster_id AND job_run_duration.usage_date=cluster_cost_duration.usage_date )
GROUP BY ALL;

CREATE OR REPLACE VIEW  finops.system_lookups.v_job_creator_weighted_cost AS 
SELECT creator_duration.*,
-- (sum of creator task duration) / (total of task duration on cluster)
-- sum(creator_duration.day_cluster_creator_task_exec_duration) creator_exec_duration,
-- min(cluster_cost_duration.day_cluster_task_exec_duration) cluster_exec_duration,
-- these are at the day level.  You need to recalculate if you change the grain of analysis
-- creator_exec_duration/cluster_exec_duration as exec_duration_cluster_pct,
-- min(cluster_cost_duration.day_cluster_est_dbu_cost)*exec_duration_cluster_pct as exec_duration_weighted_cluster_cost
-- WE ARE CREATING AN ADDITIVE, COMPOSABLE MEASURE OF COST
round(sum(creator_duration.job_creator_exec_duration) / 1000 /60,1) cluster_day_exec_duration_mins,
round(min(cluster_cost_duration.day_cluster_est_total_cost)*(sum(creator_duration.job_creator_exec_duration)/min(cluster_cost_duration.day_cluster_task_exec_duration)),4) as exec_duration_weighted_cluster_cost
FROM finops.system_lookups.v_job_creator_duration creator_duration
INNER JOIN finops.system_lookups.v_cluster_cost AS cluster_cost_duration
ON (creator_duration.cluster_id=cluster_cost_duration.cluster_id AND creator_duration.usage_date=cluster_cost_duration.usage_date )
GROUP BY ALL;



-- final apportionment
CREATE OR REPLACE VIEW finops.system_lookups.v_cost_by_job_creator_daily_apportionment AS 
SELECT job_run_creator,
DATE(creator_weighted_cost.usage_date) usage_date,
sum(exec_duration_weighted_cluster_cost) creator_exec_duration_weighted_cluster_cost,
-- use min to avoid double counting.  This measure cannot be aggregated
ROUND(creator_exec_duration_weighted_cluster_cost/min(tots.total_exec_duration_weighted_cluster_cost) ,4) as daily_pct_cost
FROM finops.system_lookups.v_job_creator_weighted_cost creator_weighted_cost
-- cartesian
INNER JOIN (
  SELECT sum(exec_duration_weighted_cluster_cost) as total_exec_duration_weighted_cluster_cost, usage_date
  FROM finops.system_lookups.v_job_creator_weighted_cost
  GROUP BY usage_date
) as tots ON (tots.usage_date=creator_weighted_cost.usage_date)
GROUP BY job_run_creator,DATE(creator_weighted_cost.usage_date)
ORDER BY usage_date ASC, sum(exec_duration_weighted_cluster_cost) DESC;



