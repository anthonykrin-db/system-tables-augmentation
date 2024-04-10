# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

spark.sql(f'CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}') 
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME}') 
# optional: add location 
# spark.sql(f'CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} LOCATION {DATABASE_LOCATION}') 
