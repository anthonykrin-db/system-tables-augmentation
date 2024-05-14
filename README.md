#System table augmentation

## Objectives

This light-weight project augments system tables with data that is not yet captured (as of May, 2024).  Efforts have been made to decouple dashboard views from underlying tables to allow evolution as system tables expand and this project becomes unecessary.

It also archives system tables to allow unlimited data retention.

The intent of this project is facilitate decommissioning the heavy-weight Overwatch project.

## Usage
<ol>
<li>Please set configuration found in in <b>00_Config</b></li>
<li>Create a Workflow that runds these notebooks in the following order, not more than once per hour:<br>
<ol><i>
<li>notebooks/03-ArchiveSystemTables</li>
<li>notebooks/04-ExtractDims-Core</li>
<li>notebooks/05-ExtractDims-Extras</li>
<li>notebooks/06-UpdateViews</li></i>
</ol>
</li>

<li>If the schema specified in configuration is not empty, make sure that you can tolerate tables starting with <b>system_</b> being created there.</li>
<li>Download and install dashboards in /dashboards folder.</li>
</ol>

## Permissions

Note that this solution uses Databricks APIs to retrieve data. 

## Configuration
Edit these entries in 00_Config Note that if these configurations are changed, hard-coded references in views must be changed
```
################
# Critical configuration
DATABASE_NAME = "finops"
SCHEMA_NAME = "system_lookups"
################
# Workspace in which to read system tables and write observability schema, tables, and views
WORKSPACE_HOST = 'https://adb-984752964297111.11.azuredatabricks.net'
################
# Service account token for API use
AUTH_HEADER = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")}
################
# Configurable discount for each month, can skip
# Be sure to include a distant end date
DBU_DISCOUNTS = {
 "01-2020":"0.235",
 "01-2030":"0.235"
}
################
# Configurable infrastructure markup for each month can skp
# Be sure to include a distant end date
# Typical markups are 75% of DBU including storage fees
INFRA_MARKUPS = {
 "01-2020":"0.65",
 "03-2024":"0.55",
 "01-2030":"0.55"
}
################
# Workspace ID and name pairs.  Unfortunately, this is not yet exposedin system tables.
WORKSPACE_NAMES = {
"6024433575559853":"WKSP59853",
"6058900950367176":"WKSP67176",
"2574677666339144":"WKSP39144"
}
```

## Backlog
Check for gaps in case jobs don't finish but incremental field updates. Remove hard coded references to schema/table names in views

