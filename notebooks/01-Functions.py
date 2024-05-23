# Databricks notebook source
# MAGIC %md
# MAGIC `Loading imports`

# COMMAND ----------

# DBTITLE 1,Imports
import requests
import time
import json
import hashlib
import re
import pandas as pd
from datetime import date, datetime, timedelta
from pyspark.sql.functions import from_unixtime, lit, json_tuple, explode, current_date, current_timestamp
from delta.tables import *


# COMMAND ----------

# DBTITLE 1,Force merge
dbutils.widgets.dropdown("param_force_merge_incremental", "False", ["True", "False"], "Force merge")
FORCE_MERGE_INCREMENTAL = dbutils.widgets.get("param_force_merge_incremental")
print(f"FORCE_MERGE_INCREMENTAL: {FORCE_MERGE_INCREMENTAL}")

# COMMAND ----------

# MAGIC %md
# MAGIC `Loading spark settings`

# COMMAND ----------

# MAGIC %md
# MAGIC `Loading public methods`

# COMMAND ----------

def update_table_schema(df,db_name,table_name):
  empty_df = df.filter("1 = 0")
  empty_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(db_name + "." + table_name)

# COMMAND ----------

def get_api_endpoints():
  return [(url, access_token) for url, access_token in WORKSPACE_API_ENDPOINTS.items()]

# COMMAND ----------

def parse_workspaceid_from_api_endpoint_url(url):
    workspace_id = re.search(r"adb-(\d+)", url).group(1)
    return workspace_id

# COMMAND ----------

def execute_multiline_sql_file(path):
    # Read the contents of the SQL file
    sql_statements = None
    with open(path, "r") as file:
        sql_statements = file.read()

    if sql_statements is None:
        print("No sql statements found.")

    # Split the SQL statements by semicolon (;)
    statements = sql_statements.split(";")

    # Execute each SQL statement
    for statement in statements:
        # Skip empty statements
        if statement.strip():
            if statement.startswith("--"):
                print(f"Skipping statement: {statement}")
            elif "VIEW" in statement:
                try:
                    print(f"\n============================\nExecuting statement: {statement}\n")
                    spark.sql(statement)
                except Exception as e:
                    print("Exception found: ", e)
            else:
                print(f"Ignoring statement: {statement}")

# COMMAND ----------

# DBTITLE 1,Config densification
from pyspark.sql.functions import from_unixtime, lit, json_tuple, explode, current_date, current_timestamp
from datetime import datetime, timedelta
from decimal import Decimal

def densify_monthly_config_df(money_struct,field_name):

  # Convert the DBU_DISCOUNT data structure to a list of tuples
  discount_data = [(datetime.strptime(k, "%m-%Y"), Decimal(v)) for k, v in money_struct.items()]

  # Sort the discount data by date
  discount_data.sort(key=lambda x: x[0])

  # Generate the date range
  start_date = discount_data[0][0].replace(day=1)
  end_date = discount_data[-1][0].replace(day=1)

  current_date = start_date
  current_discount = None

  # Initialize the result list
  result = []

  # Iterate over the date range
  while current_date <= end_date:
      # Check if there is a discount value for the current date
      discount_found = False
      for date, discount in discount_data:
          if date == current_date:
              current_discount = discount
              discount_found = True
              break

      # If no discount value is found, use the previous discount value
      result.append((current_date.replace(day=1), current_discount))

      # Move to the next month
      current_date += timedelta(days=32)
      current_date = current_date.replace(day=1)

  # Create a pandas DataFrame from the result
  pdf_discounts = pd.DataFrame(result, columns=["month_date", field_name])

  # Convert the pandas DataFrame to a Spark DataFrame
  discounts_df = spark.createDataFrame(pdf_discounts) \
      .withColumn("month_date", date_format(col("month_date"), "yyyy-MM-dd").cast("date")) \
      .withColumn("snapshot_time", current_timestamp())

  return discounts_df

# COMMAND ----------

# DBTITLE 1,Lookup last value
def lookup_last_record_value(db_name, table_name, field_name):
  last_value = None
  try:
    last_value_sql = f"SELECT {field_name} FROM {db_name}.{table_name} ORDER BY {field_name} DESC LIMIT 1"
    print("Looking up last record value: ",last_value_sql)
    last_value_df=spark.sql(last_value_sql)
    last_value = last_value_df.first()[0]
    #print("last_value: {}",last_value)
  except Exception as e:
    print("Unable to get last value: {}",e)
  return last_value
  
def lookup_last_workspace_record_value(db_name, table_name, workspace_id, field_name):
  last_value = None
  try:
    last_value_sql = f"SELECT {field_name} FROM {db_name}.{table_name} WHERE workspace_id='{workspace_id}' ORDER BY {field_name} DESC LIMIT 1"
    print("Looking up last record value: ",last_value_sql)
    last_value_df=spark.sql(last_value_sql)
    last_value = last_value_df.first()[0]
    #print("last_value: {}",last_value)
  except Exception as e:
    print("Unable to get last value: {}",e)
  return last_value

# COMMAND ----------

def count_duplicates(db_name, table_name, field_name):
  num_dups = 0
  try:
    num_dups_sql = f"WITH cte AS ( SELECT {field_name}, ROW_NUMBER() OVER(PARTITION BY {field_name} ORDER BY {field_name}) AS rowno FROM {db_name}.{table_name} ) SELECT COUNT(*) FROM cte WHERE rowno > 1"
    num_dups_df=spark.sql(num_dups_sql)
    num_dups = num_dups_df.first()[0]
    print("Found {} duplicate records",num_dups)
  except Exception as e:
    print("Unable to get last value: {}",e)
  return num_dups

def delete_duplicates(db_name, table_name, field_name):
  num_dups = count_duplicates(db_name, table_name, field_name)
  if num_dups == 0:
    return None
  del_dups_result=None
  try:
    delete_dups_sql=f"WITH cte AS ( SELECT {field_name}, ROW_NUMBER() OVER(PARTITION BY {field_name} ORDER BY {field_name}) AS rowno FROM {db_name}.{table_name}) DELETE FROM {db_name}.{table_name} WHERE {field_name} IN (SELECT {field_name} FROM cte WHERE rowno > 1)"
    del_dups_result=spark.sql(delete_dups_sql)
  except Exception as e:
    print("Unable to delete duplicates: {}",e)
  return del_dups_result

# COMMAND ----------

def commmit_data_array(data, include, exclude, db_name, table_name):
  try:
    if len(data)>0:
      combined_df = json_documents_combined_panda(data,include,exclude)
      dump_pandas_info(combined_df)
      # print("parsed_json: {}".format(parsed_json))
      df = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())
      print("Saving table: {}.{}".format(db_name, table_name))
      df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(db_name + "." + table_name)
    else:
      print("No data")
  except Exception as e:
    print("An error occurred: {}".format(str(e)))

# COMMAND ----------

def append_merge( all_objs, include, exclude, db_name, table_name, pk_field_name):

  if len(all_objs)>0:
    # Objects will be combined on the driver node
    combined_df=json_documents_combined_panda(all_objs,include,exclude)
    dump_pandas_info(combined_df)
    # print("parsed_json: {}".format(parsed_json))
    df = spark.createDataFrame(combined_df).withColumn("snapshot_time", current_timestamp())
    # Objects will be atonomically written
  else:
    print("No data")
    return
  
  #check if table is empty
  row_count=0
  try:
    row_count = spark.table(f"{db_name}.{table_name}").count()
  except Exception as e:
    print("Exception counting rows: ",e)
  if row_count == 0:
    print(f"No values found.  Creating new table {table_name}.")
    df.dropDuplicates().write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(db_name + "." + table_name) 
  else:
    delete_result=delete_duplicates(db_name,table_name,pk_field_name)
    print(f"Checking for duplicate values in {table_name} on {pk_field_name}.")
    display(delete_result)
    if FORCE_MERGE_INCREMENTAL is True:
      print(f"Table values found.  Merging new values into table {table_name}.")
      df.dropDuplicates().createOrReplaceTempView("tmp")
      # key field: object_id
      # Work around to deal with 
      update_table_schema(df,db_name,table_name)
      merge_sql="MERGE INTO {}.{} AS target USING {} AS source ON target.{} = source.{} WHEN NOT MATCHED THEN INSERT *".format(db_name,table_name, "tmp",pk_field_name,pk_field_name)
      print("Using merge SQL: ",merge_sql)
      spark.sql(merge_sql)  
    else:
      print(f"Table values found. Appending incremental values on table {table_name}.")
      # Append data and merge schema
      df.dropDuplicates().write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(db_name + "." + table_name)

# COMMAND ----------

def json_documents_combined_panda(json_docs, explode_keys=[], exclude_keys=[]):
    # Iterate over the outer array in the JSON data
    dfs = json_documents_to_pandas(json_docs, explode_keys, exclude_keys)
    return combine_dataframes(dfs)

def json_documents_to_pandas(json_docs, explode_keys=[], exclude_keys=[]):
    parsed_jsons = parse_documents(json_docs)
    dfs = []
    for doc in parsed_jsons:
        data = {}
        if isinstance(doc, list):
            # doc is list...
            for docEl in doc:
                dataEl = get_safe_json_kv(docEl, explode_keys, exclude_keys)
                df = pd.DataFrame(dataEl)
                dfs.append(df)
        else:
            if isinstance(doc, str):
                # we should never be here
                data = doc
            else:
                # fix this error: ValueError: If using all scalar values, you must pass an index
                data = get_safe_json_kv(doc, explode_keys, exclude_keys)
                df = pd.DataFrame(data)
                dfs.append(df)
    return dfs

# COMMAND ----------

# DBTITLE 1,dump out panda metadata
import pandas as pd

def dump_pandas_info(df):
  print("Number of rows:", df.shape[0])
  print("\nColumn metadata:")
  df.info()

# COMMAND ----------

# MAGIC %md
# MAGIC `Loading private methods`

# COMMAND ----------

def get_safe_json_kv(doc, explode_keys=[], exclude_keys=[]):
    data = {}

    if isinstance(doc, dict):
        for key, value in doc.items():
            if explode_keys and key in explode_keys:
                # print("explode_keys: {}, key: {}".format(explode_keys,key))
                if value.items():
                    for sub_key, sub_value in value.items():
                        # print("sub_key: {}, sub_value: {}".format(sub_key,sub_value))
                        safe_key = make_spark_column_name(sub_key)
                        safe_value = make_sdf_safe_value(sub_value)
                        # print("safe_key: {} not in exclude_keys: {} ".format(safe_key,exclude_keys))
                        if (safe_key not in exclude_keys):
                            data[safe_key] = [safe_value]
            else:
                # Get cleaned column names
                safe_key = make_spark_column_name(key)
                safe_value = make_sdf_safe_value(value)
                # print("safe_key: {} not in exclude_keys: {} ".format(safe_key,exclude_keys))
                if (safe_key not in exclude_keys):
                    data[safe_key] = [safe_value]
    else:
        print("Doc is not a dict: {}".format(doc))
        data["default"] = doc

    return data


# COMMAND ----------

def make_spark_column_name(col):
    """
    Generate a valid and unique column name for Spark.

    Args:
        col (str): The original column name.
        existing_cols (list): A list of existing column names.

    Returns:
        str: A valid and unique column name for Spark.
    """
    # Remove invalid characters and replace dots with underscores
    new_col_name = re.sub(r'[\W_]+', '_', col)
    return new_col_name

# COMMAND ----------

import math

def make_sdf_safe_value(value):
    if isinstance(value, bool):
        # If the value is a boolean, cast it to lowercase string
        return str(value).lower()
    elif isinstance(value, (int,float)):
        if math.isnan(value):
            return None
        return value
    elif isinstance(value, str):
        if value.strip() == "":
            return None
        return value
    elif isinstance(value, (list,dict)):
        # Convert Python to JSON value
        return json.dumps(value)
    else:
        return value


# COMMAND ----------

import pandas as pd
import re
from collections import Counter
import numpy as np

def replace_empty_string(value):
    if pd.isna(value) or (isinstance(value, str) and (value.strip() == "" or value.strip()=="nan")):
        return None
    return value


def combine_dataframes(dfs, ignore_index=False):
    """
    Combine an array of pandas DataFrames into one DataFrame with non-duplicative column names.
    Supports complex column values like sets.
    Ensures column names are valid and unique for Spark.
    Coerces data types based on the input DataFrames' column types.

    Args:
        dfs (list): A list of pandas DataFrames to combine.
        ignore_index (bool, optional): If True, the resulting DataFrame will have a new index.
                                       If False (default), the original indices are preserved.

    Returns:
        pandas.DataFrame: The combined DataFrame with non-duplicative column names and coerced data types.
    """
    # Get the list of column names from all DataFrames
    all_columns = []
    for df in dfs:
        all_columns.extend([str(col) for col in df.columns])

    print("Combining {} dataframes".format(len(dfs)))

    # Create a set to remove duplicates
    unique_columns = set(all_columns)

    # Create a new DataFrame with unique column names
    combined_df = pd.DataFrame(columns=list(unique_columns))

    # Combine the DataFrames
    for df in dfs:
        # Convert complex column values to strings
        #for col in df.columns:
        #    df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (set, frozenset)) else x)
        # Combine the DataFrames
        combined_df = pd.concat([combined_df, df], axis=0, join='outer')

    # Coerce data types based on the input DataFrames' column types
    for col in combined_df.columns:
        #print(col)
        column_types = [df[col].dtype for df in dfs if col in df.columns]
        column_types = Counter(column_types)

        if len(column_types) == 1:
            dtype = column_types.most_common(1)[0][0]
            if np.issubdtype(dtype, np.integer):
                combined_df[col] = combined_df[col].fillna(-1).astype(dtype)
            else:
                combined_df[col] = combined_df[col].astype(dtype)
        elif any(dtype == 'array' for dtype in column_types):
            combined_df[col] = combined_df[col].apply(lambda x: pd.array([x]) if pd.notna(x) and not isinstance(x, list) else pd.array(x) if pd.notna(x) else pd.array([]))
        elif any(dtype == 'object' for dtype in column_types):
            combined_df[col] = combined_df[col].apply(lambda x: x if pd.notna(x) else {})

    # Coalescing data types
    coalesced_df = coalesce_column_types(combined_df)
    return coalesced_df.applymap(replace_empty_string)


# COMMAND ----------

import pandas as pd
import numpy as np

def coalesce_column_types(df):
    for column in df.columns:
        # Get the distinct data types in the column
        column_types = df[column].apply(type).unique()
        
        # Define a mapping of data types to their complexity level
        type_complexity = {
            int: 1,
            np.int64: 2,
            float: 3,
            np.float64: 4,
            str: 5,
            list: 6,
            dict: 7
        }
        
        # Find the most complex data type in the column
        most_complex_type = max(column_types, key=lambda x: type_complexity.get(x, 0))
        
        #print("Column: {}, types: {}, most complex types: {}".format(column,column_types,most_complex_type))

        # Coalesce the column to the most complex data type
        if most_complex_type == int:
            df[column] = df[column].apply(lambda x: int(x) if pd.notnull(x) else x)
        elif most_complex_type == np.int64:
            df[column] = df[column].apply(lambda x: np.int64(x) if pd.notnull(x) else x)
        elif most_complex_type == float:
            df[column] = df[column].apply(lambda x: float(x) if pd.notnull(x) else x)
        elif most_complex_type == np.float64:
            df[column] = df[column].apply(lambda x: np.float64(x) if pd.notnull(x) else x)
        elif most_complex_type == str:
            df[column] = df[column].astype(str)
        elif most_complex_type == list:
            df[column] = df[column].apply(lambda x: x if isinstance(x, list) else [x])
        elif most_complex_type == dict:
            df[column] = df[column].apply(lambda x: x if isinstance(x, dict) else {'value': x})
    
    return df


# COMMAND ----------

import json
import pandas as pd

def parse_documents(json_documents):
    results = []
    
    for doc in json_documents:
        results.append(parse_doc(doc))
    return results

#Unnest deeply nested

def parse_doc(doc):
    parsed_doc = {}
    
    if isinstance(doc, str):
        parsed_doc = doc
    elif hasattr(doc, 'items'):
        for key, value in doc.items():
            if isinstance(value, bool):
                value = str(value).lower()
            # Unnest multi-level docs
            if "." in key:
                parts = key.split(".")
                if len(parts) >= 2:
                    nested_key = ".".join(parts[:2])
                    remaining_key = ".".join(parts[2:])
                    if nested_key not in parsed_doc:
                        parsed_doc[nested_key] = {}
                    parsed_doc[nested_key][remaining_key] = value
            else:
                parsed_doc[key] = value
    else:
        # Check for simple types
        if isinstance(doc, bool):
            parsed_doc = str(doc).lower()
        elif isinstance(doc, (int, float)):
            parsed_doc = doc
        elif isinstance(doc, list):
            parsed_doc = [parse_doc(item) for item in doc]
        elif isinstance(doc, dict):
            parsed_doc = {key: parse_doc(value) for key, value in doc.items()}
    
    return parsed_doc

# COMMAND ----------

# DBTITLE 1,Turn API results into json
def result_to_json(result):
  return json.dumps(result.json())

# COMMAND ----------

# DBTITLE 1,Get specific page results (Dashboards API only)
def get_page_result(base_url, page, auth):
  return requests.get(f'{base_url}&page={page}&order=executed_at', headers=auth)

# COMMAND ----------

# DBTITLE 1,Get specific offset results (Workflows API only)
def get_offset_result(base_url, offest, auth):
  return requests.get(f'{base_url}&offset={offest}', headers=auth)
