SELECT usage.usage_metadata as ALL_METADATA
FROM finops.system_lookups.v_system_usage_cost usage
LIMIT 100;

-- 0
SELECT COUNT (DISTINCT usage.usage_metadata["job_id"]) as NUM_JOBS
FROM finops.system_lookups.v_system_usage_cost usage;


SELECT COUNT (DISTINCT *) as NUM_JOBS_2
FROM finops.system_lookups.v_jobs;

-- 0
SELECT COUNT (DISTINCT usage.usage_metadata["run_id"]) as NUM_JOB_RUNS
FROM finops.system_lookups.v_system_usage_cost usage;

-- 0
SELECT COUNT (DISTINCT *) as NUM_JOB_RUNS_2
FROM finops.system_lookups.v_job_runs;

SELECT COUNT (DISTINCT usage.usage_metadata["cluster_id"]) as NUM_CLUSTERS
FROM finops.system_lookups.v_system_usage_cost usage;

SELECT COUNT (DISTINCT usage.usage_metadata["warehouse_id"]) as NUM_WAREHOUSES
FROM finops.system_lookups.v_system_usage_cost usage;

SELECT COUNT (DISTINCT usage.usage_metadata["instance_pool_id"]) as NUM_INSTANCE_POOLS
FROM finops.system_lookups.v_system_usage_cost usage;

SELECT COUNT (DISTINCT usage.usage_metadata["job_run_id"]) as NUM_JOB_RUNS
FROM finops.system_lookups.v_system_usage_cost usage;

SELECT COUNT (DISTINCT usage.usage_metadata["notebook_id"]) as NUM_NOTEBOOKS
FROM finops.system_lookups.v_system_usage_cost usage;

SELECT COUNT (DISTINCT usage.usage_metadata["dlt_pipeline_id"]) as NUM_DLT_PIPELINES
FROM finops.system_lookups.v_system_usage_cost usage;


