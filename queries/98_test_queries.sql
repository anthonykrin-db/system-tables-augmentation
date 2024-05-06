
-- 1179: unmatched job runs
SELECT count(*) unmatched_job_runs
FROM akrinsky_dbsql_logging.finops.v_job_runs vr
LEFT JOIN akrinsky_dbsql_logging.finops.v_jobs vj ON vr.job_id = vj.job_id
WHERE vj.job_id IS NULL;

-- 4768: matched job runs
SELECT count(*) atched_job_runs
FROM akrinsky_dbsql_logging.finops.v_job_runs vr
JOIN akrinsky_dbsql_logging.finops.v_jobs vj ON vr.job_id = vj.job_id;

-- 88: unmatched job runs 
SELECT count(*) unmatched_job_run_tasks
FROM akrinsky_dbsql_logging.finops.v_job_runs vr
LEFT JOIN akrinsky_dbsql_logging.finops.v_job_runs_tasks vrt ON vr.run_id = vrt.run_id
WHERE vrt.run_id IS NULL;

-- 0: unmatched job runs 
SELECT count(*) unmatched_job_run_tasks
FROM akrinsky_dbsql_logging.finops.v_job_runs_tasks vrt  
LEFT JOIN akrinsky_dbsql_logging.finops.v_job_runs vr ON vr.run_id = vrt.run_id
WHERE vrt.run_id IS NULL;


-- final apportionment
SELECT * FROM akrinsky_dbsql_logging.finops.v_cost_byshared_cluster_apportionment;

-- check jobs
SELECT * FROM akrinsky_dbsql_logging.finops.v_shared_cluster_creator_job_duration;

SELECT DISTINCT data_security_mode, access_mode FROM  akrinsky_dbsql_logging.finops.v_clusters;

