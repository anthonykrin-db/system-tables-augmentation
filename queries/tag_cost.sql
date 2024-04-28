

-- render % breakdown
SELECT businessUnit, ROUND(bu_cost/total_cost,2) as portion_pct
FROM akrinsky_dbsql_logging.finops.v_cost_bytag_apportionment 