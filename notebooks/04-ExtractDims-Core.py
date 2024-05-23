# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

# MAGIC %run ./02-Initialization

# COMMAND ----------

# DBTITLE 1,Jobs
for ENDPOINT_URL, cred in URL_CREDS:
  AUTH_HEADER = {"Authorization" : "Bearer " + cred}
  workspace_id = parse_workspaceid_from_api_endpoint_url(ENDPOINT_URL)
  print(f"Downloading jobs from workspace: {workspace_id} at {ENDPOINT_URL}")

  # Jobs (one shot)
  response = requests.get(f"{ENDPOINT_URL}{JOBS_URL}", headers=AUTH_HEADER)

  if response.status_code != 200:
    raise Exception(response.text)
  response_json = response.json()

  data=[]
  count=0
  while response_json["jobs"]:
    jobs_json = response_json["jobs"]
    for job_json in jobs_json:
      job_json["workspace_id"]=workspace_id
    data.append(jobs_json)
    count=count+1
    if (MAX_PAGES_PER_RUN<count):
      print("Reached max number of pages")
      break
    next_page_token = None
    if "next_page_token" in response_json:
      next_page_token=response_json["next_page_token"]
      url=f'{ENDPOINT_URL}{JOBS_URL}?page_token={next_page_token}'
      #print("Calling: {}".format(url))
      response = requests.get(url, headers=AUTH_HEADER)
      #print(response)
      response_json = response.json()
    else:
      break

commmit_data_array(data, ["settings","schedule","deployment","email_notifications"], [], DATABASE_NAME, JOBS_TABLE_NAME)

# COMMAND ----------

# DBTITLE 1,Job Run

# Get last JOB_RUNS start_time
###########################
# start_time_from int64
# Show runs that started at or after this value. The value must be a UTC timestamp in milliseconds.  
# start_time_to int64
# Show runs that started at or before this value. The value must be a UTC timestamp in milliseconds. 


#run_type=JOB_RUN
#run_type=WORKFLOW_RUN
#run_type=SUBMIT_RUN


count = 0

for ENDPOINT_URL, cred in URL_CREDS:
  # Reset variables for every workspace
  jobs_runs_incremental_url=None
  job_runs_json=[]
  tasks_json=[]
  
  AUTH_HEADER = {"Authorization" : "Bearer " + cred}
  workspace_id = parse_workspaceid_from_api_endpoint_url(ENDPOINT_URL)
  print(f"Downloading job runs from workspace: {workspace_id} at {ENDPOINT_URL}")
  last_job_run_start_time = lookup_last_workspace_record_value(DATABASE_NAME,JOB_RUNS_TABLE_NAME, workspace_id, "start_time")
          
  if last_job_run_start_time is None:
    print("Unable to get last job start time.  Getting all job runs.")
    # Default rest API no incremental
    jobs_runs_incremental_url = f"{ENDPOINT_URL}{JOB_RUNS_URL}?expand_tasks=true"
  else:
    print("Getting workspace objects after last modified at time: ",str(last_job_run_start_time))
    jobs_runs_incremental_url = f"{ENDPOINT_URL}{JOB_RUNS_URL}?expand_tasks=true&start_time_from={last_job_run_start_time}"

  # Job runs (fetches by page, with tasks)
  # Supports time based fetching
  response = requests.get(f"{ENDPOINT_URL}{JOB_RUNS_URL}?expand_tasks=true", headers=AUTH_HEADER)

  if response.status_code != 200:
    raise Exception(response.text)
  response_json = response.json()

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
      url=f"{ENDPOINT_URL}{JOB_RUNS_URL}?expand_tasks=true&page_token={next_page_token}"
      #print("Calling: {}".format(url))
      response = requests.get(url, headers=AUTH_HEADER)
      response_json = response.json()
    else:
      print("next_page_token not found.")
      break

  ###################################################
  # extract tasks
  for job_run_json in job_runs_json:
      job_run_json["workspace_id"] = workspace_id
      task_index = 0
      if "tasks" in job_run_json:
          for task_json in job_run_json["tasks"]:
              task_json["job_id"] = job_run_json["job_id"]
              task_json["run_id"] = job_run_json["run_id"]
              task_json["workspace_id"] = workspace_id
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
  append_merge( job_runs_json,["settings","state","schedule"],["tasks"],last_job_run_start_time, DATABASE_NAME, JOB_RUNS_TABLE_NAME, "run_id")


  ###################################################
  # write tasks
  append_merge( tasks_json,["cluster_instance","state"],[],last_job_run_start_time, DATABASE_NAME, JOB_RUNS_TABLE_NAME+"_TASKS", "task_id")


# COMMAND ----------

# DBTITLE 1,pinned clusters
# Return information about all pinned clusters, active clusters, up to 200 of the most recently terminated all-purpose clusters in the past 30 days, and up to 30 of the most recently terminated job clusters in the past 30 days.
# For example, if there is 1 pinned cluster, 4 active clusters, 45 terminated all-purpose clusters in the past 30 days, and 50 terminated job clusters in the past 30 days, then this API returns the 1 pinned cluster, 4 active clusters, all 45 terminated all-purpose clusters, and the 30 most recently terminated job clusters.
# Clusters (one shot)

data = []

for ENDPOINT_URL, cred in URL_CREDS:
  AUTH_HEADER = {"Authorization" : "Bearer " + cred}
  workspace_id = parse_workspaceid_from_api_endpoint_url(ENDPOINT_URL)
  print(f"Downloading pinned clusters from workspace: {workspace_id} at {ENDPOINT_URL}")
          
  response = requests.get(f"{ENDPOINT_URL}{CLUSTERS_URL}", headers=AUTH_HEADER)

  if response.status_code != 200:
    raise Exception(response.text)
  response_json = response.json()

  if "clusters" in response_json:
    cluster_jsons = response_json["clusters"]
    for cluster_json in cluster_jsons:
      cluster_json["workspace_id"]=workspace_id
    data.append(cluster_jsons)

commmit_data_array(data, ["azure_attributes"], [], DATABASE_NAME, CLUSTERS_TABLE_NAME)

# COMMAND ----------


