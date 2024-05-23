# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

# MAGIC %run ./02-Initialization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Include jars
# MAGIC Be sure to include jars in cluster condfiguration
# MAGIC
# MAGIC ```
# MAGIC /Volumes/finops/system_lookups/libs/gudusoft.gsqlparser-2.8.5.8.jar
# MAGIC /Volumes/finops/system_lookups/libs/sql_parser_excl_gudu_v01.jar```

# COMMAND ----------

from py4j.java_gateway import java_import
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType

java_import(spark._jvm, "parser.ColumnToken") 
java_import(spark._jvm, "parser.ParseResult") 
java_import(spark._jvm, "parser.SqlStatementParser")

parser = spark._jvm.SqlStatementParser()

# COMMAND ----------

# DBTITLE 1,Parsing Test


sql = "with y as (SELECT * FROM x.tbl_a) select a,b,c,DATETOSTRING(date_attr_name,'mm/dd/yyyy') from y"

result = parser.parseTokens(sql)

if result.isSuccess() is True:

  print("Success: " + str(result.isSuccess())) 
  for token in result.getTokens(): 
    print("Schema: " + str(token.getSchema())) 
    print("Table: " + str(token.getTable())) 
    print("Column: " + str(token.getColumn())) 
    print("-------------\n")

else: print("Error: " + result.getErrMsg())

# COMMAND ----------

# DBTITLE 1,Parse query history
# Assuming a workaround or placeholder for the SQL parsing logic
def parse_sql(sql):
    tokens = []
    result = parser.parseTokens(sql)
    if result.isSuccess() is True:
        print("Success: " + str(result.isSuccess())) 
        for token in result.getTokens(): 
            # Placeholder parsing logic
            tokens.append(token)  # Simplified example
    return tokens

# Simplified UDF usage assuming parse_sql is adjusted as suggested



# Incremental record check
query_history_end_time = lookup_last_record_value (DATABASE_NAME, f"{SYSTEM_TABLE_SCHEMA_PREFIX}.{SQL_COLUMNS_TABLE _NAME}", "event_time")
if query_history_end_time is None:
    print("Unable to get last query parsed. Getting all queries.")
else:
    print(f"Got last query parsed: {}")

 query_statement_sql=f"SELECT * FROM
 (source_table_path}"
 else:
 print("Parsed query statement history.")
 query_statement_sql=f"SELECT * FROM
 {source_table_path) WHERE end_time>
 (query_history_end_time?"
print (f"query_statement_sql:
{query_statement_sql}")
# Statement
# finops.system_query.history.statement_text
query_statements_df=spark.sql
(query_statement_sql+" LIMIT 100"')
# Apply the function parse_sql to the
"statement_text" column in query_statements_df
parse_sql_udf = F.udf (parse_sql, ArrayType
-(StringType()))
query_statements_df = query_statements_df.
withColumn("parsed_tokens", parse_sql_udf(F.
 col("statement_text") ))
 display(query_statements_df)


parse_sql_udf = udf(parse_sql, ArrayType(StringType()))
query_statements_df = query_statements_df.withColumn("parsed_tokens", parse_sql_udf(col("statement_text")))

display(query_statements_df)
