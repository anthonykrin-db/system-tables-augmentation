--- how many nodes are used per hour
CREATE OR REPLACE VIEW finops.system_lookups.v_nodes_per_hour AS (
SELECT DATE(date_trunc('HOUR',start_time)) metric_date,  HOUR(date_trunc('HOUR',start_time)) metric_hour, CASE
WHEN driver = true THEN 'driver'
ELSE 'worker'
END AS worker_driver, count(distinct instance_id) num_instances
FROM system.compute.node_timeline
GROUP BY worker_driver, metric_date, metric_hour
ORDER BY  metric_date ASC, metric_hour ASC, worker_driver ASC
);

-- peak usage
CREATE OR REPLACE VIEW finops.system_lookups.v_peak_usage AS (
WITH instance_counts AS (
SELECT
date_trunc('HOUR', start_time) AS metric_date_hour,
DATE(date_trunc('HOUR', start_time)) AS metric_date,
HOUR(date_trunc('HOUR', start_time)) AS metric_hour,
CASE
WHEN driver = TRUE THEN 'driver'
ELSE 'worker'
END AS worker_driver,
node_type AS machine_type,
COUNT(DISTINCT instance_id) AS num_instances
FROM system.compute.node_timeline
GROUP BY worker_driver, metric_date_hour, metric_date, metric_hour, machine_type
)
SELECT *
FROM instance_counts
WHERE (metric_date_hour, num_instances) IN (
SELECT metric_date_hour, MAX(num_instances)
FROM instance_counts
GROUP BY metric_date_hour
));

-- TODO: test with real node_timeline date
CREATE OR REPLACE VIEW finops.system_lookups.v_usage_minute_matrix AS 

WITH 
node_timeline AS (
  SELECT * FROM system.compute.node_timeline
),
time_table AS (
SELECT
EXPLODE(SEQUENCE(
MIN(DATE_TRUNC('MINUTE', start_time)) - INTERVAL 1 MINUTES,
MAX(DATE_TRUNC('MINUTE', start_time)),
INTERVAL 1 MINUTES
)) AS time_metric_minute
FROM node_timeline
GROUP BY node_timeline.workspace_id
),
base_table AS (
SELECT
DATE_TRUNC('MINUTE', start_time) AS base_metric_minute,
  SUM(t.network_received_bytes) AS network_received_bytes,
  SUM(t.network_sent_bytes) AS network_sent_bytes,
  SUM(t.mem_used_percent*nt.memory_mb) memory_mb_used,
  SUM(t.cpu_user_percent*nt.core_count) cpu_cores_used,
  SUM(nt.memory_mb) tot_memory_mb_used,
  SUM(nt.core_count) tot_cpu_cores_used,
  t.instance_id, t.node_type, t.workspace_id
FROM node_timeline t LEFT OUTER JOIN finops.system_compute.node_types nt ON nt.node_type=t.node_type
GROUP BY t.workspace_id, t.instance_id,  t.node_type, DATE_TRUNC('MINUTE', start_time)
)
SELECT
  t.base_metric_minute,
  SUM(t.network_received_bytes) AS network_received_bytes,
  SUM(t.network_sent_bytes) AS network_sent_bytes,
  SUM(t.memory_mb_used) memory_mb_used,
  SUM(t.cpu_cores_used) cpu_cores_used,
  SUM(t.tot_memory_mb_used) tot_memory_mb_used,
  SUM(t.tot_cpu_cores_used) tot_cpu_cores_used,
  t.instance_id, t.node_type, t.workspace_id
FROM base_table t LEFt OUteR JOIN finops.system_compute.node_types nt ON nt.node_type=t.node_type
GROUP BY t.workspace_id, t.instance_id,  t.node_type, t.base_metric_minute;
