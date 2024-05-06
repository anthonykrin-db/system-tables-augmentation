CREATE OR REPLACE VIEW finops.system_lookups_dims.v_shared_cluster_job_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT 
    jr.job_name, 
    jr.job_id,
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    cl.cluster_name,
    cl.data_security_mode cluster_data_security_mode,
    cl.cluster_source,
    c.usage_date,
    SUM(jrt.execution_duration) AS day_cluster_job_task_exec_duration
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
    -- Filtering on billing record in last 30 days
    WHERE c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY  jr.job_id, jr.job_name, jrt.cluster_id, cl.cluster_name,c.usage_date, w.workspace_id, 
    w.workspace_name, cl.data_security_mode,cl.cluster_source;


-- intermediary query 3: weighted cost of creator jobs on each cluster, as well as % cluster used (assumes only tasks run on clusters)
CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_shared_cluster_job_duration_weighted_cost AS 
SELECT job_duration.job_name, 
job_duration.usage_date,
job_duration.cluster_id,
job_duration.cluster_name,
job_duration.cluster_data_security_mode,
job_duration.cluster_source,
cluster_cost_duration.workspace_name,
-- (sum of creator task duration) / (total of task duration on cluster)
sum(job_duration.day_cluster_job_task_exec_duration) creator_exec_duration,
min(cluster_cost_duration.day_cluster_task_exec_duration) cluster_exec_duration,
creator_exec_duration/cluster_exec_duration as exec_duration_cluster_pct,
min(cluster_cost_duration.day_cluster_est_dbu_cost)*exec_duration_cluster_pct as exec_duration_weighted_cluster_cost
FROM finops.system_lookups_dims.v_shared_cluster_job_duration job_duration
INNER JOIN finops.system_lookups_dims.v_shared_cluster_job_duration_cost AS cluster_cost_duration
ON (job_duration.cluster_id=cluster_cost_duration.cluster_id AND job_duration.usage_date=cluster_cost_duration.usage_date )
GROUP BY job_duration.job_name, job_duration.usage_date,job_duration.cluster_id, job_duration.cluster_name, job_duration.cluster_data_security_mode,job_duration.cluster_source,cluster_cost_duration.workspace_name;


CREATE OR REPLACE VIEW finops.system_lookups_dims.v_shared_cluster_job_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT 
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
    -- Filtering on billing record in last 30 days
    WHERE c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY jr.job_name, jrt.cluster_id, cl.cluster_name,c.usage_date, w.workspace_id, 
    w.workspace_name, cl.data_security_mode,cl.cluster_source;


-- cost by job-run
-- For jobs that run on their own job clusters
CREATE OR REPLACE VIEW finops.system_lookups_dims.v_cost_byjob_cluster_apportionment AS 
SELECT j.creator_user_name AS job_runner, 
min(agg_cost.total_cost) total_cost,
ROUND(sum(est_dbu_cost)/min(agg_cost.total_cost),4) period_pct_cost
FROM finops.system_lookups_dims.v_system_usage_cost
INNER JOIN finops.system_lookups_dims.v_job_runs j 
ON j.run_id = usage_metadata["job_run_id"]
INNER JOIN 
  (
    SELECT SUM(c.est_dbu_cost) AS total_cost
    FROM finops.system_lookups_dims.v_system_usage_cost c
    WHERE c.usage_metadata["job_run_id"] is not null
      AND c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS agg_cost
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY j.creator_user_name;

-- v_cost_byworkspace
CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_cost_byworkspace AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, sum(c.est_dbu_cost) est_dbu_cost
FROM  finops.system_lookups_dims.v_system_usage_cost c
INNER JOIN finops.system_lookups_dims.v_workspaces w ON (c.workspace_id=w.workspace_id)
GROUP BY c.workspace_id, w.workspace_name, c.usage_date, c.sku_name;

-- v_cost_bycluster
CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_cost_bycluster AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, sum(c.est_dbu_cost) est_dbu_cost, cl.cluster_id, cl.cluster_name
FROM  finops.system_lookups_dims.v_system_usage_cost c
INNER JOIN finops.system_lookups_dims.v_workspaces w ON (c.workspace_id=w.workspace_id)
INNER JOIN finops.system_lookups_dims.v_clusters cl ON (cl.cluster_id=c.usage_metadata["cluster_id"])
GROUP BY c.workspace_id, w.workspace_name, c. usage_date, c.sku_name,cl.cluster_id, cl.cluster_name;







