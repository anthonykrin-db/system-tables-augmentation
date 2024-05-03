# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

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

#print("Saving table: {}.{}".format(DATABASE_NAME, CLUSTERS_TABLE_NAME))
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
  job_runs_json.append(response_json["runs"])
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
for job_run_batch_json in job_runs_json:
  for job_run_json in job_run_batch_json:
    print("JOB RUN: ",job_run_json)
    task_index=0
    for task_json in job_run_json["tasks"]:
      task_json["job_id"]=job_run_json["job_id"]
      task_index=task_index+1
      task_json["task_index"]=task_index
      tasks_json.append(task_json)

###################################################
# write job runs
runs_combined_df = json_documents_combined_panda(job_runs_json,["settings","state","schedule"],["tasks"])
dump_pandas_info(runs_combined_df)

job_runs = spark.createDataFrame(runs_combined_df).withColumn("snapshot_time", current_timestamp())

job_runs.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + JOB_RUNS_TABLE_NAME)

###################################################
# write tasks
tasks_combined_df = json_documents_combined_panda(tasks_json,["cluster_instance"],[])
dump_pandas_info(tasks_combined_df)

job_run_tasks = spark.createDataFrame(tasks_combined_df).withColumn("snapshot_time", current_timestamp())

job_run_tasks.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + JOB_RUNS_TABLE_NAME+"_TASKS")




# COMMAND ----------

# DBTITLE 1,Settings
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true") # needed for query history API
spark.sql("SET spark.databricks.delta.properties.defaults.minReaderVersion = 2") # needed for workspace API
spark.sql("SET spark.databricks.delta.properties.defaults.minWriterVersion = 5") # needed for workspace API
spark.sql("SET spark.databricks.delta.properties.defaults.columnMapping.mode = name") # needed for workspace API

# COMMAND ----------

# DBTITLE 1,Delta Pipelines
import json
import pandas as pd
import requests

response = requests.get(DLT_PIPELINES_URL, headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)

response_json = response.json()
#print(response_json)

data=[]
while response_json["statuses"]:
  data.append(response_json["statuses"])

  next_page_token = None
  if "next_page_token" in response_json:
    next_page_token=response_json["next_page_token"]
    url=f'{DLT_PIPELINES_URL}?page_token={next_page_token}'
    #print("Calling: {}".format(url))
    response = requests.get(url, headers=AUTH_HEADER)
    #print(response)
    response_json = response.json()
  else:
    break

if len(data)>0:
  combined_df = json_documents_combined_panda(data,[],["latest_updates"])
  dump_pandas_info(combined_df)
  # print("parsed_json: {}".format(parsed_json))
  dlt_pipelines = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())

  ##print("Saving table: {}.{}".format(DATABASE_NAME, CLUSTERS_TABLE_NAME))
  dlt_pipelines.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + DLT_PIPELINES_TABLE_NAME)
else:
  print("No data")



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

  ##print("Saving table: {}.{}".format(DATABASE_NAME, CLUSTERS_TABLE_NAME))
  clusters.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + CLUSTERS_TABLE_NAME)

# COMMAND ----------

# DBTITLE 1,intance pools
import json
import pandas as pd

response = requests.get(INSTANCE_POOLS_URL, headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()
#print(response_json)
if "instance_pools" in response_json:
  combined_df = json_documents_combined_panda(response_json["instance_pools"])
  dump_pandas_info(combined_df)
  pools = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())
  ##print("Saving table: {}.{}".format(DATABASE_NAME, WORKSPACE_OBJECTS_TABLE_NAME))
  pools.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + INSTANCE_POOLS_TABLE_NAME)

# COMMAND ----------

# DBTITLE 1,Warehouses
import json
import pandas as pd

response = requests.get(WAREHOUSES_URL, headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()
# print(response_json)

if "warehouses" in response_json:
  combined_df = json_documents_combined_panda(response_json["warehouses"])
  dump_pandas_info(combined_df)
  whs = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())

  ##print("Saving table: {}.{}".format(DATABASE_NAME, CLUSTERS_TABLE_NAME))
  whs.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + WAREHOUSES_TABLE_NAME)

# COMMAND ----------

# DBTITLE 1,Dashboards

## there is no next page token with this API so will need to iterate thorough all results until we get empty results

base_url = f'{DASHBOARDS_URL}/admin?page_size={PAGE_SIZE}'
empty_response = {'message': 'Page is out of range.'}

data = []
# initial request to set up the objects
res = get_page_result(base_url, 1, AUTH_HEADER)
responses_json = res.json()
if "results" in responses_json and responses_json["results"]:
    #print(responses_json["results"])
    data.append(parse_doc(responses_json["results"]))
# print(f'PAGE 1 done')
    page = 2
    while responses_json != empty_response and responses_json["count"]>0:
        res = get_page_result(base_url, page, AUTH_HEADER)#
        responses_json = res.json()
        if "results" in responses_json and responses_json["results"]:
          #print(responses_json["results"])
          data.append(parse_doc(responses_json["results"]))
        #print(f'PAGE {page} done')
        page += 1

if len(data)>0:
  combined_df = json_documents_combined_panda(data,["options"])

  dump_pandas_info(combined_df)

  # print("parsed_json: {}".format(parsed_json))
  dashs = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())

  ##print("Saving table: {}.{}".format(DATABASE_NAME, CLUSTERS_TABLE_NAME))
  dashs.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + DASHBOARDS_TABLE_NAME)
else:
  print("No data")


# COMMAND ----------

# DBTITLE 1,notebooks
import json
import pandas as pd

def get_path_objs(path,depth=-1):
  params = {}
  if depth==-1:
    params = {
        "path": path  # Specify the root directory
    }
  else:
    params = {
        "path": path,  # Specify the root directory
        "depth": depth    # Limit the depth to 1 to retrieve only top-level directories
    }

  response = requests.get(WORKSPACE_OBJECTS_URL, headers=AUTH_HEADER, params=params)

  if response.status_code != 200:
      raise Exception(response.text)
  
  response_json = response.json()
  json = []
  if "objects" in response_json:
    json = response_json["objects"]
  return json


top_level_objects=get_path_objs("/",1)
# This will ignore directory path
    
all_objs = []
# Print the top-level directory paths
for top_level_object in top_level_objects:
    #print(top_level_object)
    if top_level_object["object_type"] != "DIRECTORY":
      all_objs.append(top_level_object)
    else:
      dir_name=top_level_object["path"]
      print("Dir name: {}".format(dir_name))
      if dir_name == "/Users":
        user_dir_json = get_path_objs(dir_name,1)
        for user_dir in user_dir_json:
          print("User path: {}".format(user_dir["path"]))
          user_objsn = get_path_objs(user_dir["path"])
          all_objs.append(user_objsn)
      else:
        other_dir_objs = get_path_objs(dir_name)
        all_objs.append(other_dir_objs)

combined_df=json_documents_combined_panda(all_objs)
dump_pandas_info(combined_df)
# print("parsed_json: {}".format(parsed_json))
notebooks = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())

##print("Saving table: {}.{}".format(DATABASE_NAME, WORKSPACE_OBJECTS_TABLE_NAME))
notebooks.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + WORKSPACE_OBJECTS_TABLE_NAME)
