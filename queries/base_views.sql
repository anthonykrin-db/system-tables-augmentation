-- v_system_usage_cost
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_system_usage_cost AS
SELECT usage.usage_metadata, usage.usage_quantity, list_prices.pricing["default"] list_cost, usage_date, usage.sku_name, workspace_id
FROM system.billing.usage usage
    INNER JOIN system.billing.list_prices list_prices on
      usage.cloud = list_prices.cloud and
      usage.sku_name = list_prices.sku_name and
      usage.usage_start_time >= list_prices.price_start_time and
      (usage.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null)

