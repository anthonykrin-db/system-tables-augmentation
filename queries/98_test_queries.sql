
-- 58
SELECT count(*) cnt FROM finops.system_lookups.v_shared_cluster_creator_job_duration;

-- 0: unmatched job runs
SELECT count(*) unmatched_job_runs
FROM finops.system_lookups.v_job_runs vr
LEFT JOIN finops.system_lookups.v_jobs vj ON vr.job_id = vj.job_id
WHERE vj.job_id IS NULL;

-- 5346: matched job runs
SELECT count(*) atched_job_runs
FROM finops.system_lookups.v_job_runs vr
JOIN finops.system_lookups.v_jobs vj ON vr.job_id = vj.job_id;

-- 0: unmatched job runs 
SELECT count(*) unmatched_job_run_tasks
FROM finops.system_lookups.v_job_runs vr
LEFT JOIN finops.system_lookups.v_job_runs_tasks vrt ON vr.run_id = vrt.run_id
WHERE vrt.run_id IS NULL;

-- 0: unmatched job runs 
SELECT count(*) unmatched_job_run_tasks
FROM finops.system_lookups.v_job_runs_tasks vrt  
LEFT JOIN finops.system_lookups.v_job_runs vr ON vr.run_id = vrt.run_id
WHERE vrt.run_id IS NULL;
