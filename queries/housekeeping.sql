
-- verify tables
SELECT * FROM akrinsky_dbsql_logging.finops.clusters
SELECT * FROM akrinsky_dbsql_logging.finops.dashboards_preview
SELECT * FROM akrinsky_dbsql_logging.finops.dlt_pipelines
SELECT * FROM akrinsky_dbsql_logging.finops.instance_pools
SELECT * FROM akrinsky_dbsql_logging.finops.jobs
SELECT * FROM akrinsky_dbsql_logging.finops.warehouses

-- create views
CREATE OR REPLACE VIEW akrinsky_dbsql_logging.finops.clusters as SELECT * FROM akrinsky_dbsql_logging.tacklebox.clusters;
CREATE  OR REPLACE VIEW  akrinsky_dbsql_logging.finops.dashboards_preview as SELECT * FROM akrinsky_dbsql_logging.tacklebox.dashboards_preview;
CREATE  OR REPLACE VIEW  akrinsky_dbsql_logging.finops.dlt_pipelines as SELECT * FROM akrinsky_dbsql_logging.tacklebox.dlt_pipelines;
CREATE  OR REPLACE VIEW  akrinsky_dbsql_logging.finops.instance_pools as SELECT * FROM akrinsky_dbsql_logging.tacklebox.instance_pools;
CREATE  OR REPLACE VIEW  akrinsky_dbsql_logging.finops.jobs as SELECT * FROM akrinsky_dbsql_logging.tacklebox.jobs;
CREATE  OR REPLACE VIEW  akrinsky_dbsql_logging.finops.warehouses as SELECT * FROM akrinsky_dbsql_logging.tacklebox.warehouses;

-- ctas tables
CREATE TABLE akrinsky_dbsql_logging.finops.clusters as SELECT * FROM akrinsky_dbsql_logging.tacklebox.clusters
CREATE TABLE akrinsky_dbsql_logging.finops.dashboards_preview as SELECT * FROM akrinsky_dbsql_logging.tacklebox.dashboards_preview
CREATE TABLE akrinsky_dbsql_logging.finops.dlt_pipelines as SELECT * FROM akrinsky_dbsql_logging.tacklebox.dlt_pipelines
CREATE TABLE akrinsky_dbsql_logging.finops.instance_pools as SELECT * FROM akrinsky_dbsql_logging.tacklebox.instance_pools
CREATE TABLE akrinsky_dbsql_logging.finops.jobs as SELECT * FROM akrinsky_dbsql_logging.tacklebox.jobs
CREATE TABLE akrinsky_dbsql_logging.finops.warehouses as SELECT * FROM akrinsky_dbsql_logging.tacklebox.warehouses

-- drop tables
DROP TABLE IF EXISTS akrinsky_dbsql_logging.tacklebox.jobs;
DROP TABLE  IF EXISTS  akrinsky_dbsql_logging.tacklebox.clusters;
DROP TABLE  IF EXISTS akrinsky_dbsql_logging.tacklebox.warehouses;
DROP  TABLE  IF EXISTS akrinsky_dbsql_logging.tacklebox.workspace_objects;
DROP  TABLE  IF EXISTS akrinsky_dbsql_logging.tacklebox.dashboards_preview;
DROP  TABLE  IF EXISTS akrinsky_dbsql_logging.tacklebox.instance_pools;
