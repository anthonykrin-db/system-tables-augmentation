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
