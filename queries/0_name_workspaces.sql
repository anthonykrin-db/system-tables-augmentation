-- Workspace names will be coming here:  system.access.workspaces
-- For now, we must create manually

CREATE OR REPLACE TABLE akrinsky_dbsql_logging.finops.workspaces
AS 
SELECT DISTINCT WORKSPACE_ID, 
CASE  
WHEN WORKSPACE_ID='1347839109641223' THEN 'FOO'
-- follow this pattern 
ELSE CONCAT("WKSP",RIGHT(WORKSPACE_ID,5)) END
WORKSPACE_NAME 
FROM akrinsky_dbsql_logging.finops.v_system_usage_cost;

CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.v_workspaces as
SELECT * FROM akrinsky_dbsql_logging.finops.workspaces;