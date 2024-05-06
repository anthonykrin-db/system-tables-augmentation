# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

# DBTITLE 1,Settings
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true") # needed for query history API
spark.sql("SET spark.databricks.delta.properties.defaults.minReaderVersion = 2") # needed for workspace API
spark.sql("SET spark.databricks.delta.properties.defaults.minWriterVersion = 5") # needed for workspace API
spark.sql("SET spark.databricks.delta.properties.defaults.columnMapping.mode = name") # needed for workspace API

# COMMAND ----------

# MAGIC %run ./02-Initialization

# COMMAND ----------

# DBTITLE 1,Imports
import requests
import time
import json
from datetime import date, datetime, timedelta
from pyspark.sql.functions import from_unixtime, lit, json_tuple, explode, current_date, current_timestamp
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Jobs
import json
import pandas as pd
import requests

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
import json
import pandas as pd
import requests

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
            task_index = task_index + 1
            task_json["task_index"] = task_index
            tasks_json.append(task_json)
    else:
        print("No tasks for job run: {}".format(job_run_json["run_id"]))


###################################################
# write job runs
runs_combined_df = json_documents_combined_panda(job_runs_json,["settings","state","schedule"],["tasks"])
dump_pandas_info(runs_combined_df)
job_runs = spark.createDataFrame(runs_combined_df).withColumn("snapshot_time", current_timestamp())
print("Saving table: {}.{}".format(DATABASE_NAME, JOB_RUNS_TABLE_NAME))
job_runs.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + JOB_RUNS_TABLE_NAME)

###################################################
# write tasks
tasks_combined_df = json_documents_combined_panda(tasks_json,["cluster_instance"],[])
dump_pandas_info(tasks_combined_df)
job_run_tasks = spark.createDataFrame(tasks_combined_df).withColumn("snapshot_time", current_timestamp())
print("Saving table: {}.{}".format(DATABASE_NAME, JOB_RUNS_TABLE_NAME+"_TASKS"))
job_run_tasks.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + JOB_RUNS_TABLE_NAME+"_TASKS")




# COMMAND ----------

# DBTITLE 1,clusters
import json
import pandas as pd

response = requests.get(CLUSTERS_URL, headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()


if "clusters" in response_json:
  combined_df = json_documents_combined_panda(response_json["clusters"],["azure_attributes"])
  dump_pandas_info(combined_df)
  # print("parsed_json: {}".format(parsed_json))
  clusters = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())
  print("Saving table: {}.{}".format(DATABASE_NAME, CLUSTERS_TABLE_NAME))
  clusters.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + CLUSTERS_TABLE_NAME)
else:
  print("No data")
