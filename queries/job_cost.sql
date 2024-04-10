SELECT usage_date, 
      jobs.name, 
      sum(usage.usage_quantity*usage.list_cost) cost,
      
  FROM akrinsky_dbsql_logging.finops.v_system_usage_cost usage
      INNER join akrinsky_dbsql_logging.tacklebox.jobs jobs on
      jobs.job_id = usage.usage_metadata["job_id"]
GROUP BY usage_date, jobs.name
