


-- v_system_usage_cost (start here)
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_system_usage_cost AS
SELECT usage.custom_tags, usage.usage_metadata, usage.usage_quantity, list_prices.pricing["default"] list_cost, usage_date, usage.sku_name, workspace_id
FROM system.billing.usage usage
    INNER JOIN system.billing.list_prices list_prices on
      usage.cloud = list_prices.cloud and
      usage.sku_name = list_prices.sku_name and
      usage.usage_start_time >= list_prices.price_start_time and
      (usage.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null)


-- create breakdowns by a custom_tags.<tag> for prior 30 days
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_cost_bytag_apportionment AS (
SELECT custom_tags.x_Owner businessUnit, sum(list_cost) bu_cost, min(agg_cost.total_cost) total_cost
--, ROUND(tag_cost/total_cost) pct_total
from akrinsky_dbsql_logging.finops.v_system_usage_cost, 
(
SELECT sum(list_cost) total_cost
from akrinsky_dbsql_logging.finops.v_system_usage_cost
WHERE  usage_date >= DATE_SUB(CURRENT_DATE(), 30)
) as agg_cost
WHERE  usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY custom_tags.x_Owner
)

-- render % breakdown
SELECT businessUnit, ROUND(bu_cost/total_cost,2) as portion_pct
FROM akrinsky_dbsql_logging.finops.v_cost_bytag_apportionment 