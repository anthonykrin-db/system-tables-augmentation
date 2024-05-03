-- v_cost_byworkspace
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_cost_byworkspace AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, sum(c.est_dbu_cost) est_dbu_cost
FROM  akrinsky_dbsql_logging.finops.v_system_usage_cost c
INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w ON (c.workspace_id=w.workspace_id)
GROUP BY c.workspace_id, w.workspace_name, c.usage_date, c.sku_name;

-- v_cost_bycluster
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_cost_bycluster AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, sum(c.est_dbu_cost) est_dbu_cost, cl.cluster_id, cl.cluster_name
FROM  akrinsky_dbsql_logging.finops.v_system_usage_cost c
INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w ON (c.workspace_id=w.workspace_id)
INNER JOIN akrinsky_dbsql_logging.finops.v_clusters cl ON (cl.cluster_id=c.usage_metadata["cluster_id"])
GROUP BY c.workspace_id, w.workspace_name, c. usage_date, c.sku_name,cl.cluster_id, cl.cluster_name;

-- v_cost_byjob
CREATE OR REPLACE VIEW  akrinsky_dbsql_logging.finops.v_cost_bycluster AS
SELECT c.workspace_id, w.workspace_name, c.usage_date, c.sku_name, sum(c.est_dbu_cost) est_dbu_cost, cl.cluster_id, cl.cluster_name
FROM  akrinsky_dbsql_logging.finops.v_system_usage_cost c
INNER JOIN akrinsky_dbsql_logging.finops.v_workspaces w ON (c.workspace_id=w.workspace_id)
INNER JOIN akrinsky_dbsql_logging.finops.v_clusters cl ON (cl.cluster_id=c.usage_metadata["cluster_id"])
GROUP BY c.workspace_id, w.workspace_name, c. usage_date, c.sku_name,cl.cluster_id, cl.cluster_name;



