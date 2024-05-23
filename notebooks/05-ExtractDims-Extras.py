# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

# MAGIC %run ./02-Initialization

# COMMAND ----------

# DBTITLE 1,Delta Pipelines

data=[]

for ENDPOINT_URL, cred in URL_CREDS:
  AUTH_HEADER = {"Authorization" : "Bearer " + cred}
  workspace_id = parse_workspaceid_from_api_endpoint_url(ENDPOINT_URL)
  print(f"Downloading delta pipelines from workspace: {workspace_id} at {ENDPOINT_URL}")
            
  response = requests.get(f"{ENDPOINT_URL}{DLT_PIPELINES_URL}", headers=AUTH_HEADER)
  if response.status_code != 200:
    raise Exception(response.text)
  response_json = response.json()
  #print(response_json)
  while response_json["statuses"]:
    dlt_json_pools = response_json["statuses"]
    for dlt_json_pool in dlt_json_pools:
      dlt_json_pool["workspace_id"]=workspace_id
    data.append(dlt_json_pools)

    next_page_token = None
    if "next_page_token" in response_json:
      next_page_token=response_json["next_page_token"]
      url=f"{ENDPOINT_URL}{DLT_PIPELINES_URL}?page_token={next_page_token}"
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

    print("Saving table: {}.{}".format(DATABASE_NAME, DLT_PIPELINES_TABLE_NAME))
    dlt_pipelines.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + DLT_PIPELINES_TABLE_NAME)
  else:
    print("No data")

commmit_data_array(data, [], [], DATABASE_NAME, DLT_PIPELINES_TABLE_NAME)



# COMMAND ----------

# DBTITLE 1,intance pools
data=[]

for ENDPOINT_URL, cred in URL_CREDS:
  AUTH_HEADER = {"Authorization" : "Bearer " + cred}
  workspace_id = parse_workspaceid_from_api_endpoint_url(ENDPOINT_URL)
  print(f"Downloading instance pools from workspace: {workspace_id} at {ENDPOINT_URL}")

  response = requests.get(f"{ENDPOINT_URL}{INSTANCE_POOLS_URL}", headers=AUTH_HEADER)
  if response.status_code != 200:
    raise Exception(response.text)
  responses_json = response.json()

  instance_pool_jsons = responses_json["instance_pools"]
  for instance_pool_json in instance_pool_jsons:
    instance_pool_json["workspace_id"]=workspace_id
  data.append(instance_pool_jsons)

commmit_data_array(data, [], [], DATABASE_NAME, INSTANCE_POOLS_TABLE_NAME)

# COMMAND ----------

# DBTITLE 1,Dashboards

## there is no next page token with this API so will need to iterate thorough all results until we get empty results

data = []

for ENDPOINT_URL, cred in URL_CREDS:
  AUTH_HEADER = {"Authorization" : "Bearer " + cred}  
  workspace_id = parse_workspaceid_from_api_endpoint_url(ENDPOINT_URL)
  print(f"Downloading dashboards from workspace: {workspace_id} at {ENDPOINT_URL}")

  base_url = f"{ENDPOINT_URL}{DASHBOARDS_URL}/admin?page_size={PAGE_SIZE}"
  empty_response = {'message': 'Page is out of range.'}


  # initial request to set up the objects
  res = get_page_result(base_url, 1, AUTH_HEADER)
  responses_json = res.json()
  if "results" in responses_json and responses_json["results"]:
      #print(responses_json["results"])
      data.append(responses_json["results"])
  # print(f'PAGE 1 done')
      page = 2
      while responses_json != empty_response and responses_json["count"]>0:
          res = get_page_result(base_url, page, AUTH_HEADER)#
          responses_json = res.json()
          if "results" in responses_json and responses_json["results"]:
            #print(responses_json["results"])
            dashboard_jsons = responses_json["results"]
            for dashboard_json in dashboard_jsons:
              dashboard_json["workspace_id"]=workspace_id
            data.append(dashboard_jsons)
          #print(f'PAGE {page} done')
          page += 1


commmit_data_array(data,["options"], [], DATABASE_NAME, DASHBOARDS_TABLE_NAME)


# COMMAND ----------

# DBTITLE 1,Warehouses
data=[]

for ENDPOINT_URL, cred in URL_CREDS:
  AUTH_HEADER = {"Authorization" : "Bearer " + cred}
  workspace_id = parse_workspaceid_from_api_endpoint_url(ENDPOINT_URL)
  print(f"Downloading warehouses from workspace: {workspace_id} at {ENDPOINT_URL}")

  response = requests.get(f"{ENDPOINT_URL}{WAREHOUSES_URL}", headers=AUTH_HEADER)

  if response.status_code != 200:
    raise Exception(response.text)
  response_json = response.json()
  # print(response_json)
  if "warehouses" in response_json:
    warehouse_jsons = response_json["warehouses"]
    for warehouse_json in warehouse_jsons:
      warehouse_json["workspace_id"]=workspace_id
    data.append(warehouse_jsons)

commmit_data_array(data,[], [], DATABASE_NAME, WAREHOUSES_TABLE_NAME)

# COMMAND ----------

# DBTITLE 1,notebooks

# Recursive function to get workplace objects
def get_path_objs(path,url, workspace_id, depth=-1):
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

  response = requests.get(url, headers=AUTH_HEADER, params=params)

  if response.status_code != 200:
      raise Exception(response.text)
    
  response_json = response.json()
  json = []
  if "objects" in response_json:
    json = response_json["objects"]
    for obj_json in json:
      obj_json["workspace_id"]=workspace_id
  return json

for ENDPOINT_URL, cred in URL_CREDS:
  # reset variables
  all_objs=[]
  AUTH_HEADER = {"Authorization" : "Bearer " + cred}
  workspace_id = parse_workspaceid_from_api_endpoint_url(ENDPOINT_URL)
  print(f"Downloading workspace objects from workspace: {workspace_id} at {ENDPOINT_URL}")
  last_modified_at = lookup_last_workspace_record_value(DATABASE_NAME,WORKSPACE_OBJECTS_TABLE_NAME,workspace_id, "modified_at"):
    
  print(f"Downloading workspace objects from workspace: {ENDPOINT_URL}")
          
  # Default rest API no incremental
  workspace_objects_incremental_url = f"{ENDPOINT_URL}{WORKSPACE_OBJECTS_URL}"
  if last_modified_at is None:
    print("Unable to get last modified at time.  Getting all workspace objects.")
  else:
    print("Getting workspace objects after last modified at time.")
    workspace_objects_incremental_url = f"{ENDPOINT_URL}{WORKSPACE_OBJECTS_URL}?notebooks_modified_after="+str(last_modified_at)

  # method starts here
  top_level_objects=get_path_objs("/",workspace_objects_incremental_url,workspace_id,1)
  # This will ignore directory path

  # Print the top-level directory paths
  for top_level_object in top_level_objects:
      #print(top_level_object)
      if top_level_object["object_type"] != "DIRECTORY":
        all_objs.append(top_level_object)
      else:
        dir_name=top_level_object["path"]
        print("Dir name: {}".format(dir_name))
        if dir_name == "/Users":
          user_dir_json = get_path_objs(dir_name,workspace_objects_incremental_url,workspace_id,1)
          for user_dir in user_dir_json:
            print("User path: {}".format(user_dir["path"]))
            user_objsn = get_path_objs(user_dir["path"],workspace_objects_incremental_url,workspace_id)
            all_objs.append(user_objsn)
        else:
          other_dir_objs = get_path_objs(dir_name,workspace_objects_incremental_url,workspace_id)
          all_objs.append(other_dir_objs)

  append_merge( all_objs,[],[],last_modified_at, DATABASE_NAME, WORKSPACE_OBJECTS_TABLE_NAME, "object_id")

