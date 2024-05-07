-- apportioned cost by jobs running on shared clusters
-- For jobs that run on shared job clusters
-- intermediary query 1 (each creator)
CREATE OR REPLACE VIEW finops.system_lookups_dims.v_shared_cluster_creator_job_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT 
    jr.creator_user_name AS job_run_creator, 
    jr.job_id,
    jr.job_name,
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    cl.cluster_name,
    cl.data_security_mode cluster_data_security_mode,
    cl.cluster_source,
    c.usage_date,
    SUM(jrt.execution_duration) AS day_cluster_creator_task_exec_duration
    FROM finops.system_lookups_dims.v_system_usage_cost c 
    -- Filtering on clusters that have a billing record
    INNER JOIN finops.system_lookups_dims.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date)
    INNER JOIN finops.system_lookups_dims.v_clusters cl
      on (jrt.cluster_id=cl.cluster_id)
    INNER JOIN finops.system_lookups_dims.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    --INNER JOIN finops.system_lookups_dims.v_jobs j
    --  on (jr.job_id=j.job_id) 
    INNER JOIN finops.system_lookups_dims.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    GROUP BY jr.creator_user_name, jrt.cluster_id, cl.cluster_name,c.usage_date, w.workspace_id, 
    w.workspace_name, cl.data_security_mode,cl.cluster_source, jr.job_name, jr.job_id;

-- 58
SELECT count(*) cnt FROM finops.system_lookups_dims.v_shared_cluster_creator_job_duration;

-- SELECT * FROM finops.system_lookups_dims.v_shared_cluster_job_cost;

-- intermediary query 2 (costs and duration across all creators)
CREATE OR REPLACE VIEW finops.system_lookups_dims.v_shared_cluster_job_duration_cost AS 
    SELECT 
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    cl.cluster_name,cl.data_security_mode cluster_data_security_mode,cl.cluster_source,
    c.usage_date,
    -- all tasks on this clsuter, duration, each day
    SUM(jrt.execution_duration) AS day_cluster_task_exec_duration,
    -- total costs for this cluster, each day
    SUM(c.est_dbu_cost) AS day_cluster_est_dbu_cost
    FROM finops.system_lookups_dims.v_system_usage_cost c 
    INNER JOIN finops.system_lookups_dims.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date)
    INNER JOIN finops.system_lookups_dims.v_clusters cl
      on (jrt.cluster_id=cl.cluster_id)
    INNER JOIN finops.system_lookups_dims.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    INNER JOIN finops.system_lookups_dims.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    GROUP BY jrt.cluster_id,c.usage_date, w.workspace_id, cl.cluster_name, w.workspace_name, cl.data_security_mode,cl.cluster_source;


CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_shared_cluster_job_duration_weighted_cost AS 
SELECT 
job_duration.job_id, job_duration.job_name,
job_duration.usage_date,
job_duration.workspace_id,
job_duration.workspace_name,
job_duration.cluster_id,
job_duration.cluster_name,
job_duration.cluster_data_security_mode,job_duration.cluster_source,
round(sum(job_duration.day_cluster_creator_task_exec_duration) / 1000 /60,1) cluster_day_exec_duration_mins,
round(min(cluster_cost_duration.day_cluster_est_dbu_cost)*(sum(job_duration.day_cluster_creator_task_exec_duration)/min(cluster_cost_duration.day_cluster_task_exec_duration)),4) as exec_duration_weighted_cluster_cost
FROM finops.system_lookups_dims.v_shared_cluster_creator_job_duration job_duration
INNER JOIN finops.system_lookups_dims.v_shared_cluster_job_duration_cost AS cluster_cost_duration
ON (job_duration.cluster_id=cluster_cost_duration.cluster_id AND job_duration.usage_date=cluster_cost_duration.usage_date )
GROUP BY job_duration.usage_date,job_duration.cluster_id, job_duration.cluster_name, job_duration.cluster_data_security_mode,job_duration.cluster_source,job_duration.job_id, job_duration.job_name,job_duration.workspace_id,
job_duration.workspace_name;


CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_shared_cluster_creator_job_duration_weighted_cost AS 
SELECT creator_duration.job_run_creator, 
creator_duration.usage_date,
creator_duration.cluster_id,
creator_duration.cluster_name,
creator_duration.cluster_data_security_mode,creator_duration.cluster_source,
-- (sum of creator task duration) / (total of task duration on cluster)
-- sum(creator_duration.day_cluster_creator_task_exec_duration) creator_exec_duration,
-- min(cluster_cost_duration.day_cluster_task_exec_duration) cluster_exec_duration,
-- these are at the day level.  You need to recalculate if you change the grain of analysis
-- creator_exec_duration/cluster_exec_duration as exec_duration_cluster_pct,
-- min(cluster_cost_duration.day_cluster_est_dbu_cost)*exec_duration_cluster_pct as exec_duration_weighted_cluster_cost
-- WE ARE CREATING AN ADDITIVE, COMPOSABLE MEASURE OF COST
round(sum(creator_duration.day_cluster_creator_task_exec_duration) / 1000 /60,1) cluster_day_exec_duration_mins,
round(min(cluster_cost_duration.day_cluster_est_dbu_cost)*(sum(creator_duration.day_cluster_creator_task_exec_duration)/min(cluster_cost_duration.day_cluster_task_exec_duration)),4) as exec_duration_weighted_cluster_cost
FROM finops.system_lookups_dims.v_shared_cluster_creator_job_duration creator_duration
INNER JOIN finops.system_lookups_dims.v_shared_cluster_job_duration_cost AS cluster_cost_duration
ON (creator_duration.cluster_id=cluster_cost_duration.cluster_id AND creator_duration.usage_date=cluster_cost_duration.usage_date )
GROUP BY creator_duration.job_run_creator, creator_duration.usage_date,creator_duration.cluster_id, creator_duration.cluster_name, creator_duration.cluster_data_security_mode,creator_duration.cluster_source;



-- final apportionment
CREATE OR REPLACE VIEW finops.system_lookups_dims.v_cost_byshared_cluster_daily_apportionment AS 
SELECT job_run_creator,
DATE(creator_weighted_cost.usage_date) usage_date,
sum(exec_duration_weighted_cluster_cost) creator_exec_duration_weighted_cluster_cost,
-- use min to avoid double counting.  This measure cannot be aggregated
ROUND(creator_exec_duration_weighted_cluster_cost/min(tots.total_exec_duration_weighted_cluster_cost) ,4) as daily_pct_cost
FROM finops.system_lookups_dims.v_shared_cluster_creator_job_duration_weighted_cost creator_weighted_cost
-- cartesian
INNER JOIN (
  SELECT sum(exec_duration_weighted_cluster_cost) as total_exec_duration_weighted_cluster_cost, usage_date
  FROM finops.system_lookups_dims.v_shared_cluster_creator_job_duration_weighted_cost
  GROUP BY usage_date
) as tots ON (tots.usage_date=creator_weighted_cost.usage_date)
GROUP BY job_run_creator,DATE(creator_weighted_cost.usage_date)
ORDER BY usage_date ASC, sum(exec_duration_weighted_cluster_cost) DESC;



-- Replace x_Owner with whatever tag you need
-- create breakdowns by a custom_tags.<tag> for prior 30 days
CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_cost_bytag_daily_apportionment AS 
SELECT c.custom_tags.x_Owner businessUnit, c.usage_date,
sum(c.est_dbu_cost) bu_cost, 
min(agg_cost.total_cost) total_cost,
ROUND(sum(est_dbu_cost)/min(agg_cost.total_cost),4) period_pct_cost
from finops.system_lookups_dims.v_system_usage_cost c INNER JOIN
(
SELECT sum(uc.est_dbu_cost) total_cost, uc.usage_date
from finops.system_lookups_dims.v_system_usage_cost uc
GROUP BY uc.usage_date
) as agg_cost ON (agg_cost.usage_date=c.usage_date)
GROUP BY custom_tags.x_Owner,c.usage_date;


