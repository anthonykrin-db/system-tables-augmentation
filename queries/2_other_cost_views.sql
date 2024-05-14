

CREATE OR REPLACE VIEW finops.system_lookups.v_shared_cluster_job_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT 
    jr.job_name, 
    jr.job_id,
    jr.result_state,
    w.workspace_id, 
    w.workspace_name,
    jrt.cluster_id, 
    cl.cluster_name,
    cl.data_security_mode cluster_data_security_mode,
    cl.cluster_source,
    c.usage_date,
    SUM(jrt.execution_duration) AS day_cluster_job_task_exec_duration
    FROM finops.system_lookups.v_system_usage_cost c 
    -- Filtering on clusters that have a billing record
    INNER JOIN finops.system_lookups.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date)
    INNER JOIN finops.system_lookups.v_clusters cl
      on (jrt.cluster_id=cl.cluster_id)
    INNER JOIN finops.system_lookups.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    --INNER JOIN finops.system_lookups.v_jobs j
    --  on (jr.job_id=j.job_id) 
    INNER JOIN finops.system_lookups.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    -- Filtering on billing record in last 30 days
    WHERE c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY  jr.job_id, jr.job_name, jrt.cluster_id, cl.cluster_name,c.usage_date, w.workspace_id, 
    w.workspace_name, cl.data_security_mode,cl.cluster_source,jr.result_state;



CREATE OR REPLACE VIEW finops.system_lookups.v_shared_cluster_job_duration AS 
    -- cost of shared clusters that are involved in running jobs
    SELECT 
    jr.job_name, w.workspace_id, jr.result_state,
    w.workspace_name, jrt.cluster_id, 
    cl.cluster_name, cl.data_security_mode cluster_data_security_mode,
    cl.cluster_source, c.usage_date,
    SUM(jrt.execution_duration) AS day_cluster_creator_task_exec_duration
    FROM finops.system_lookups.v_system_usage_cost c 
    -- Filtering on clusters that have a billing record
    INNER JOIN finops.system_lookups.v_job_runs_tasks jrt 
      on (c.usage_metadata["cluster_id"]=jrt.cluster_id AND DATE(FROM_UNIXTIME(jrt.start_time / 1000)) = c.usage_date)
    INNER JOIN finops.system_lookups.v_clusters cl
      on (jrt.cluster_id=cl.cluster_id)
    INNER JOIN finops.system_lookups.v_job_runs jr 
      on (jr.run_id=jrt.run_id) 
    --INNER JOIN finops.system_lookups.v_jobs j
    --  on (jr.job_id=j.job_id) 
    INNER JOIN finops.system_lookups.v_workspaces w 
      on (c.workspace_id=w.workspace_id)
    GROUP BY jr.job_name, jrt.cluster_id, cl.cluster_name,c.usage_date, w.workspace_id, 
    w.workspace_name, cl.data_security_mode,cl.cluster_source,jr.result_state;

-- v_cost_byworkspace
CREATE OR REPLACE VIEW  finops.system_lookups.v_cost_byworkspace AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, sum(c.est_dbu_cost) est_dbu_cost
FROM  finops.system_lookups.v_system_usage_cost c
INNER JOIN finops.system_lookups.v_workspaces w ON (c.workspace_id=w.workspace_id)
GROUP BY c.workspace_id, w.workspace_name, c.usage_date, c.sku_name;

-- v_cost_bycluster
CREATE OR REPLACE VIEW  finops.system_lookups.v_cost_bycluster AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, sum(c.est_dbu_cost) est_dbu_cost, cl.cluster_id, cl.cluster_name
FROM  finops.system_lookups.v_system_usage_cost c
INNER JOIN finops.system_lookups.v_workspaces w ON (c.workspace_id=w.workspace_id)
INNER JOIN finops.system_lookups.v_clusters cl ON (cl.cluster_id=c.usage_metadata["cluster_id"])
GROUP BY c.workspace_id, w.workspace_name, c. usage_date, c.sku_name,cl.cluster_id, cl.cluster_name;







