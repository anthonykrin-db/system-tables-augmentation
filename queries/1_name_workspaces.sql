-- Workspace names will be coming here:  system.access.workspaces
-- For now, we must create manually

CREATE OR REPLACE TABLE finops.system_lookups_dims.workspaces
AS 
SELECT DISTINCT WORKSPACE_ID, 
CASE  
WHEN WORKSPACE_ID='1347839109641223' THEN 'FOO'
-- follow this pattern 
ELSE CONCAT("WKSP",RIGHT(WORKSPACE_ID,5)) END
WORKSPACE_NAME 
FROM finops.system_lookups_dims.v_system_usage_cost;

CREATE OR REPLACE VIEW finops.system_lookups_dims.v_workspaces as
SELECT * FROM finops.system_lookups_dims.workspaces;

DROP VIEW  finops.system_lookups_dims.v_workspaces;

CREATE OR REPLACE VIEW  finops.system_lookups_dims.v_workspaces as SELECT c.* FROM finops.system_lookups_dims.workspaces c;