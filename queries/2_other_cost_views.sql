-- Replace x_Owner with whatever tag you need
-- create breakdowns by a custom_tags.<tag> for prior 30 days
CREATE OR REPLACE VIEW  finops.system_lookups.v_cost_by_tag_daily_apportionment AS 
SELECT c.custom_tags.x_Owner businessUnit, c.usage_date,
sum(c.est_dbu_cost) bu_cost, 
min(agg_cost.total_cost) total_cost,
ROUND(sum(est_dbu_cost)/min(agg_cost.total_cost),4) period_pct_cost
from finops.system_lookups.v_system_usage_cost c INNER JOIN
(
SELECT sum(uc.est_dbu_cost) total_cost, uc.usage_date
from finops.system_lookups.v_system_usage_cost uc
GROUP BY uc.usage_date
) as agg_cost ON (agg_cost.usage_date=c.usage_date)
GROUP BY custom_tags.x_Owner,c.usage_date;

-- v_cost_by_workspace
CREATE OR REPLACE VIEW  finops.system_lookups.v_cost_by_workspace AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, 
sum(c.est_dbu_cost) est_tot_dbu_cost,
sum(c.est_infra_cost) est_tot_infra_const,
sum(c.est_total_cost) est_total_cost
FROM  finops.system_lookups.v_system_usage_cost c
INNER JOIN finops.system_lookups.v_workspaces w ON (c.workspace_id=w.workspace_id)
GROUP BY c.workspace_id, w.workspace_name, c.usage_date, c.sku_name;

-- v_cost_by_cluster
CREATE OR REPLACE VIEW  finops.system_lookups.v_cost_by_cluster AS
SELECT c.workspace_id, w.workspace_name, cl.cluster_id, cl.cluster_name, c.usage_date, c.sku_name, 
sum(c.est_dbu_cost) est_tot_dbu_cost,
sum(c.est_infra_cost) est_tot_infra_const,
sum(c.est_total_cost) est_total_cost
FROM  finops.system_lookups.v_system_usage_cost c
INNER JOIN finops.system_lookups.v_workspaces w ON (c.workspace_id=w.workspace_id)
INNER JOIN finops.system_lookups.v_clusters cl ON (cl.cluster_id=c.usage_metadata["cluster_id"])
GROUP BY c.workspace_id, w.workspace_name, c. usage_date, c.sku_name,cl.cluster_id, cl.cluster_name;
