-- v_system_usage_cost (start here)
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_system_usage_cost AS
SELECT usage.custom_tags, usage.usage_metadata, usage.usage_quantity, list_prices.pricing["default"] list_cost, usage_date, usage.sku_name, workspace_id
FROM system.billing.usage usage
    INNER JOIN system.billing.list_prices list_prices on
      usage.cloud = list_prices.cloud and
      usage.sku_name = list_prices.sku_name and
      usage.usage_start_time >= list_prices.price_start_time and
      (usage.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null)


-- cost by job-run
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_cost_byjobrunner_apportionment AS 
SELECT j.creator_user_name AS job_runner, SUM(list_cost) AS job_cost, MIN(agg_job_cost.total_cost) AS all_job_cost
FROM akrinsky_dbsql_logging.finops.v_system_usage_cost
INNER JOIN akrinsky_dbsql_logging.finops.job_runs j 
ON j.run_id = usage_metadata["job_run_id"]
INNER JOIN 
  (
    SELECT SUM(c.list_cost) AS total_cost
    FROM akrinsky_dbsql_logging.finops.v_system_usage_cost c
    WHERE c.usage_metadata["job_run_id"] is not null
      AND c.usage_date >= DATE_SUB(CURRENT_DATE(), 30)
  ) AS agg_job_cost
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY j.creator_user_name


-- render % breakdown
SELECT job_runner, ROUND(job_cost/all_job_cost,2) as portion_pct
FROM akrinsky_dbsql_logging.finops.v_cost_byjobrunner_apportionment 
ORDER BY portion_pct DESC