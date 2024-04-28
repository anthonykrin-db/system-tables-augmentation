# Databricks notebook source
# MAGIC %md
# MAGIC ### Loading public methods

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
# MAGIC ### Loading private methods

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
