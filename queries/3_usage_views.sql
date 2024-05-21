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

-- TODO: add workspace name
CREATE OR REPLACE VIEW finops.system_lookups.v_usage_minute_matrix AS 
WITH 
time_table AS (
SELECT
EXPLODE(SEQUENCE(
MIN(DATE_TRUNC('MINUTE', start_time)) - INTERVAL 1 MINUTES,
MAX(DATE_TRUNC('MINUTE', start_time)),
INTERVAL 1 MINUTES
)) AS time_metric_minute
FROM system.compute.node_timeline
GROUP BY node_timeline.workspace_id
),
base_table AS (
SELECT
  tt.time_metric_minute,
  t.network_received_bytes AS network_received_bytes,
  t.network_sent_bytes AS network_sent_bytes,
  t.mem_used_percent,
  t.mem_used_percent*nt.memory_mb memory_mb_used,
  t.cpu_user_percent ,
  t.cpu_system_percent ,
  t.cpu_wait_percent ,
  t.cpu_user_percent*nt.core_count cpu_user_cores_used,
  t.cpu_system_percent*nt.core_count cpu_system_cores_used,
  t.cpu_wait_percent*nt.core_count cpu_wait_cores_used,
  nt.memory_mb tot_memory_mb,
  nt.core_count tot_cpu_cores,
  t.instance_id, t.node_type, t.workspace_id
FROM time_table tt
LEFT OUTER JOIN system.compute.node_timeline t ON tt.time_metric_minute=DATE_TRUNC('MINUTE', t.start_time)
LEFT OUTER JOIN finops.system_compute.node_types nt ON nt.node_type=t.node_type
)
SELECT
  DATE(t.time_metric_minute) metric_date,
  HOUR(t.time_metric_minute) metric_hour,
  MINUTE(t.time_metric_minute) metric_minute,
  time_metric_minute,
  SUM(t.network_received_bytes) AS network_received_bytes,
  SUM(t.network_sent_bytes) AS network_sent_bytes,
  AVG(t.mem_used_percent) mem_used_percent,
  AVG(t.memory_mb_used) memory_mb_used,
  AVG(t.cpu_user_percent) cpu_user_percent,
  AVG(t.cpu_system_percent) cpu_system_percent ,
  AVG(t.cpu_wait_percent) cpu_wait_percent ,
  AVG(t.cpu_user_cores_used) cpu_user_cores_used,
  AVG(t.cpu_system_cores_used) cpu_system_cores_used,
  AVG(t.cpu_wait_cores_used) cpu_wait_cores_used,
  SUM(t.tot_memory_mb) tot_memory_mb,
  SUM(t.tot_cpu_cores) tot_cpu_cores,
  t.instance_id, t.node_type, t.workspace_id
FROM base_table t JOIN finops.system_compute.node_types nt ON nt.node_type=t.node_type
GROUP BY t.workspace_id, t.instance_id,  t.node_type, metric_date, metric_hour, metric_minute,time_metric_minute;
