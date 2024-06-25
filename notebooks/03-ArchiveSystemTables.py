# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

# DBTITLE 1,Append since last insert tables

for source_table_path, increment_column in SYSTEM_TABLES_INCREMENT.items():
  #print("source_table_path: ",source_table_path)
  #print("increment_column: ",increment_column)
  target_table_path=DATABASE_NAME+"."+SYSTEM_TABLE_SCHEMA_PREFIX+source_table_path.replace("system.","")
  target_schema_path = ".".join(target_table_path.rsplit(".", 1)[:-1])
  create_schema_sql="CREATE SCHEMA IF NOT EXISTS {}".format(target_schema_path)
  spark.sql(create_schema_sql)
  #print(create_schema_sql)
  create_table_sql="CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM {} WHERE 1=0".format(target_table_path,source_table_path)
  spark.sql(create_table_sql)
  #exotic program flow designed to facility merging schemas
  temp_view_sql="CREATE OR REPLACE TEMPORARY VIEW TMP AS (SELECT * FROM {} WHERE {} > COALESCE((SELECT MAX({}) FROM {}), '1900-01-01')) ".format(source_table_path,increment_column,increment_column,target_table_path)
  print(temp_view_sql)
  spark.sql(temp_view_sql)
  df = spark.read.table("TMP")
  result=df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table_path)
  print("Appending {} into {} on {}".format(source_table_path,target_table_path,increment_column))
  #print(result)
  #print("num_affected_rows: {}, num_inserted_rows: {} ".format(result.num_affected_rows,result.num_inserted_rows))



  




# COMMAND ----------

# DBTITLE 1,Totally replace tables
for source_table_path in SYSTEM_TABLES_REPLACE:
  #print("source_table_path: ",source_table_path)
  target_table_path=DATABASE_NAME+"."+SYSTEM_TABLE_SCHEMA_PREFIX+source_table_path.replace("system.","")
  target_schema_path = ".".join(target_table_path.rsplit(".", 1)[:-1])
  create_schema_sql="CREATE SCHEMA IF NOT EXISTS {}".format(target_schema_path)
  spark.sql(create_schema_sql)
  print("create_schema_sql: ",create_schema_sql)
  result = spark.sql("CREATE OR REPLACE TABLE {} AS SELECT * FROM {}".format(target_table_path,source_table_path))
  print("Copy table from {} to {}".format(source_table_path,target_table_path))
  print("  ",display(result))

  

