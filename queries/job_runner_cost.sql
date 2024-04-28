
-- render % breakdown
SELECT job_runner, ROUND(job_cost/all_job_cost,2) as portion_pct
FROM akrinsky_dbsql_logging.finops.v_cost_byjobrunner_apportionment 
ORDER BY portion_pct DESC