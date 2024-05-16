-- CTRL-SHIFT-ENTER to run a selected fragment;
--- create base views;

CREATE OR REPLACE VIEW  finops.system_lookups.v_jobs as SELECT j.* FROM finops.system_lookups.jobs j;

CREATE OR REPLACE VIEW  finops.system_lookups.v_job_runs as 
SELECT 
DATE(FROM_UNIXTIME(r.start_time / 1000)) usage_date, 
r.*,j.name as job_name 
FROM finops.system_lookups.job_runs r INNER JOIN finops.system_lookups.jobs j ON (j.job_id=r.job_id);

CREATE OR REPLACE VIEW  finops.system_lookups.v_job_runs_tasks as 
SELECT 
DATE(FROM_UNIXTIME(MIN(jrt.start_time) / 1000)) usage_date, 
jrt.*, j.name as job_name, IF(attempt_number>0,"REPEAT","INITIAL") attempt_type
FROM finops.system_lookups.job_runs_tasks jrt INNER JOIN finops.system_lookups.jobs j ON (j.job_id=jrt.job_id)
GROUP BY ALL;

CREATE OR REPLACE VIEW  finops.system_lookups.v_instance_pools as SELECT p.* FROM finops.system_lookups.instance_pools p;

CREATE OR REPLACE VIEW  finops.system_lookups.v_dlt_pipelines as SELECT d.* FROM finops.system_lookups.dlt_pipelines d;

-- cluster specifications change continually as noted by change_time
-- for cluster_id + time we can figure out actual configuration at that time
CREATE OR REPLACE VIEW  finops.system_lookups.v_clusters as 
SELECT c.cluster_id, c.cluster_name, coalesce(cp.data_security_mode,"") data_security_mode, 
cp.* EXCEPT(cluster_id,cluster_name,data_security_mode)
FROM (SELECT DISTINCT cluster_id, cluster_name FROM finops.system_compute.clusters) c
LEFT OUTER JOIN finops.system_lookups.clusters_pinned cp ON c.cluster_id=cp.cluster_id;

CREATE OR REPLACE VIEW  finops.system_lookups.v_dashboards_preview as SELECT c.* FROM finops.system_lookups.dashboards_preview c;

CREATE OR REPLACE VIEW finops.system_lookups.v_workspaces as
SELECT * FROM finops.system_lookups.workspaces;

-- v_system_usage_cost (start here);
CREATE OR REPLACE VIEW finops.system_lookups.v_system_usage_cost AS 
SELECT
  usage.*,
  -- discounted cost
  list_prices.pricing ["default"] est_dbu_price,
  est_dbu_price*usage.usage_quantity*(1-discounts.discount) est_dbu_cost,
  -- total cost including virtual machines
  est_dbu_cost*infra_markup.amount est_infra_cost,
  est_dbu_cost*(1+infra_markup.amount) est_total_cost
FROM
  finops.system_billing.usage usage
  INNER JOIN finops.system_billing.list_prices list_prices on usage.cloud = list_prices.cloud
  and usage.sku_name = list_prices.sku_name
  and usage.usage_start_time >= list_prices.price_start_time
  and (
    usage.usage_end_time <= list_prices.price_end_time
    or list_prices.price_end_time is null
  )
  INNER JOIN finops.system_lookups.dbu_discounts discounts on date_trunc('MONTH',usage.usage_start_time)=discounts.month_date
  INNER JOIN finops.system_lookups.infra_markups infra_markup on date_trunc('MONTH',usage.usage_start_time)=infra_markup.month_date;