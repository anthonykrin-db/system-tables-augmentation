# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

# MAGIC %run ./02-Initialization

# COMMAND ----------

# DBTITLE 1,Delta Pipelines

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

  try:
    if len(data)>0:
      combined_df = json_documents_combined_panda(data,[],["latest_updates"])
      dump_pandas_info(combined_df)
      # print("parsed_json: {}".format(parsed_json))
      dlt_pipelines = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())

      print("Saving table: {}.{}".format(DATABASE_NAME, DLT_PIPELINES_TABLE_NAME))
      dlt_pipelines.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + DLT_PIPELINES_TABLE_NAME)
    else:
      print("No data")

  except Exception as e:
      print("An error occurred: {}".format(str(e)))


# COMMAND ----------

# DBTITLE 1,intance pools

response = requests.get(INSTANCE_POOLS_URL, headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()
#print(response_json)

try:

  if "instance_pools" in response_json:
    combined_df = json_documents_combined_panda(response_json["instance_pools"])
    dump_pandas_info(combined_df)
    pools = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())
    print("Saving table: {}.{}".format(DATABASE_NAME, INSTANCE_POOLS_TABLE_NAME))
    pools.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + INSTANCE_POOLS_TABLE_NAME)
  else:
    print("No data")

except Exception as e:
  print("An error occurred: {}".format(str(e)))

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

try:
  if len(data)>0:
    combined_df = json_documents_combined_panda(data,["options"])

    dump_pandas_info(combined_df)

    # print("parsed_json: {}".format(parsed_json))
    dashs = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())

    print("Saving table: {}.{}".format(DATABASE_NAME, DASHBOARDS_TABLE_NAME))
    dashs.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + DASHBOARDS_TABLE_NAME)
  else:
    print("No data")

except Exception as e:
  print("An error occurred: {}".format(str(e)))


# COMMAND ----------

# DBTITLE 1,Warehouses


response = requests.get(WAREHOUSES_URL, headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()
# print(response_json)

try:
  
  if "warehouses" in response_json:
    combined_df = json_documents_combined_panda(response_json["warehouses"])
    dump_pandas_info(combined_df)
    whs = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())
    print("Saving table: {}.{}".format(DATABASE_NAME , WAREHOUSES_TABLE_NAME))
    whs.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + WAREHOUSES_TABLE_NAME)
  else:
    print("No data")

except Exception as e:
  print("An error occurred: {}".format(str(e)))

# COMMAND ----------

# DBTITLE 1,notebooks

# Default rest API no incremental
workspace_objects_incremental_url = WORKSPACE_OBJECTS_URL

last_modified_at = lookup_last_record_value(DATABASE_NAME,WORKSPACE_OBJECTS_TABLE_NAME, "modified_at")
if last_modified_at is None:
  print("Unable to get last modified at time.  Getting all workspace objects.")
else:
  print("Getting workspace objects after last modified at time.")
  workspace_objects_incremental_url = WORKSPACE_OBJECTS_URL+"?notebooks_modified_after="+str(last_modified_at)

# Recursive function to get workplace objects
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

  response = requests.get(workspace_objects_incremental_url, headers=AUTH_HEADER, params=params)

  if response.status_code != 200:
      raise Exception(response.text)
  
  response_json = response.json()
  json = []
  if "objects" in response_json:
    json = response_json["objects"]
  return json

# method starts here
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

try:

  if len(all_objs)>0:
    # Objects will be combined on the driver node
    combined_df=json_documents_combined_panda(all_objs)
    dump_pandas_info(combined_df)
    # print("parsed_json: {}".format(parsed_json))
    notebooks_df = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())
    # Objects will be atonomically written
    print("Saving table: {}.{}".format(DATABASE_NAME, WORKSPACE_OBJECTS_TABLE_NAME))

    if last_modified_at is None:
      print("Overwriting new table")
      notebooks_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + WORKSPACE_OBJECTS_TABLE_NAME)
    else:
      print("Updating incrementally")
      if FORCE_MERGE_INCREMENTAL:
        notebooks_df.createOrReplaceTempView("notebooks_df_table")
        # key field: object_id
        merge_sql="MERGE INTO {}.{} AS target USING {} AS source ON target.{} = source.{} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *".format(DATABASE_NAME,WORKSPACE_OBJECTS_TABLE_NAME, notebooks_df_table,"object_id","object_id")
        print("merge_sql: {}",merge_sql)
        spark.sql(merge_sql)   
      else:
        print("Appending incrementals")
        # Append data and merge schema
        notebooks_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(DATABASE_NAME + "." + WORKSPACE_OBJECTS_TABLE_NAME)


  else:
    print("No data")

except Exception as e:
  print("An error occurred: {}".format(str(e)))
