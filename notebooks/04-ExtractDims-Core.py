# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

# MAGIC %run ./02-Initialization

# COMMAND ----------

# DBTITLE 1,Prepare schema
# Create schema if does not exist
print("Creating schema [{}] if does not exist".format(DATABASE_NAME))
spark.sql(" CREATE SCHEMA IF NOT EXISTS {}".format(DATABASE_NAME))

# COMMAND ----------

# DBTITLE 1,Jobs

# Jobs (one shot)
response = requests.get(JOBS_URL, headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()

data=[]
count=0
while response_json["jobs"]:
  data.append(response_json["jobs"])
  count=count+1
  if (MAX_PAGES_PER_RUN<count):
    print("Reached max number of pages")
    break
  next_page_token = None
  if "next_page_token" in response_json:
    next_page_token=response_json["next_page_token"]
    url=f'{JOBS_URL}?page_token={next_page_token}'
    #print("Calling: {}".format(url))
    response = requests.get(url, headers=AUTH_HEADER)
    #print(response)
    response_json = response.json()
  else:
    break

combined_df = json_documents_combined_panda(data,["settings","schedule","deployment","email_notifications"])
dump_pandas_info(combined_df)

# print("parsed_json: {}".format(parsed_json))
jobs = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())
print("Saving table: {}.{}".format(DATABASE_NAME, JOBS_TABLE_NAME))
jobs.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + JOBS_TABLE_NAME)

# COMMAND ----------

# DBTITLE 1,Job Run

# Get last JOB_RUNS start_time
###########################
# start_time_from int64
# Show runs that started at or after this value. The value must be a UTC timestamp in milliseconds.  
# start_time_to int64
# Show runs that started at or before this value. The value must be a UTC timestamp in milliseconds. 

# Default rest API no incremental
jobs_runs_incremental_url = JOB_RUNS_URL+"?expand_tasks=true"

last_job_run_start_time = lookup_last_record_value(DATABASE_NAME,JOB_RUNS_TABLE_NAME, "start_time")
if last_job_run_start_time is None:
  print("Unable to get last job start time.  Getting all job runs.")
else:
  print("Getting workspace objects after last modified at time: ",str(last_job_run_start_time))
  jobs_runs_incremental_url = JOB_RUNS_URL+"?expand_tasks=true&start_time_from="+str(last_job_run_start_time)

# Job runs (fetches by page, with tasks)
# Supports time based fetching
response = requests.get(JOB_RUNS_URL+"?expand_tasks=true", headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()
job_runs_json=[]
tasks_json=[]
count = 0
print("Pages: ")
while response_json is not None and "runs" in response_json:
  for job_run_json in response_json["runs"]:
    job_runs_json.append(job_run_json)

  next_page_token = None
  count = count+1
  if (MAX_PAGES_PER_RUN<count):
    print("Reached max number of pages: {}".format(count))
    break
  if "next_page_token" in response_json:
    next_page_token=response_json["next_page_token"]
    url=f'{JOB_RUNS_URL}?expand_tasks=true&page_token={next_page_token}'
    #print("Calling: {}".format(url))
    response = requests.get(url, headers=AUTH_HEADER)
    #print(response)
    response_json = response.json()
  else:
    break

###################################################
# extract tasks
for job_run_json in job_runs_json:
    task_index = 0
    if "tasks" in job_run_json:
        for task_json in job_run_json["tasks"]:
            task_json["job_id"] = job_run_json["job_id"]
            task_json["run_id"] = job_run_json["run_id"]
            # to make key unique we must add attempt_number
            task_id_raw = str(task_json["run_id"])+"_"+task_json["task_key"]+"_"+str(task_json["attempt_number"])
            task_json["task_id"] = task_id_raw
            task_index = task_index + 1
            task_json["task_index"] = task_index
            tasks_json.append(task_json)
    else:
        print("No tasks for job run: {}".format(job_run_json["run_id"]))


###################################################
# write job runs
runs_combined_df = json_documents_combined_panda(job_runs_json,["settings","state","schedule"],["tasks"])
dump_pandas_info(runs_combined_df)
job_runs_df = spark.createDataFrame(runs_combined_df).withColumn("snapshot_time", current_timestamp())
print("Saving table: {}.{}".format(DATABASE_NAME, JOB_RUNS_TABLE_NAME))
job_runs_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + JOB_RUNS_TABLE_NAME)

append_merge(last_job_run_start_time, job_runs_df, DATABASE_NAME, JOB_RUNS_TABLE_NAME, "run_id")


###################################################
# write tasks
tasks_combined_df = json_documents_combined_panda(tasks_json,["cluster_instance","state"],[])
dump_pandas_info(tasks_combined_df)
job_run_tasks_df = spark.createDataFrame(tasks_combined_df).withColumn("snapshot_time", current_timestamp())
print("Saving table: {}.{}".format(DATABASE_NAME, JOB_RUNS_TABLE_NAME+"_TASKS"))

append_merge(last_job_run_start_time, job_run_tasks_df, DATABASE_NAME, JOB_RUNS_TABLE_NAME+"_TASKS", "task_id")


# COMMAND ----------

# DBTITLE 1,pinned clusters
# Return information about all pinned clusters, active clusters, up to 200 of the most recently terminated all-purpose clusters in the past 30 days, and up to 30 of the most recently terminated job clusters in the past 30 days.
# For example, if there is 1 pinned cluster, 4 active clusters, 45 terminated all-purpose clusters in the past 30 days, and 50 terminated job clusters in the past 30 days, then this API returns the 1 pinned cluster, 4 active clusters, all 45 terminated all-purpose clusters, and the 30 most recently terminated job clusters.
# Clusters (one shot)
response = requests.get(CLUSTERS_URL, headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()


if "clusters" in response_json:
  combined_df = json_documents_combined_panda(response_json["clusters"],["azure_attributes"])
  dump_pandas_info(combined_df)
  # print("parsed_json: {}".format(parsed_json))
  clusters_df = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())

  # Check if "data_security_mode" column exists
  if "data_security_mode" not in clusters_df.columns:
    # Add "data_security_mode" column with default value from "access_mode" column
    clusters_df = clusters_df.withColumn("data_security_mode", col("access_mode"))
    print("Saving table: {}.{}".format(DATABASE_NAME, CLUSTERS_TABLE_NAME))
    clusters_df.write.format("delta").option("overwriteShema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + CLUSTERS_TABLE_NAME)

else:
  print("No data")
