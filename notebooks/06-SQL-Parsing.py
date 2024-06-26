# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

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
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType,TimestampType

# Assuming a workaround or placeholder for the SQL parsing logic
def parse_sql(sql):
    tokens = []
    if len(sql)==0:
        return tokens
    try:
        result = parser.parseTokens(sql)
        if result.isSuccess():
            #print("Success: " + str(result.isSuccess()))
            for token in result.getTokens():
                # Placeholder parsing logic
                tokens.append(token)  # Simplified example
        else:
            print(f"Unable to parse: {result}")
    except Exception as e:
        print(f"Exception parsing {sql}: {e}")
    return tokens


limit = 100
complete = False

# SQL_COLUMNS_TABLE_NAME
query_history_start_time = lookup_last_record_value(
    DATABASE_NAME, SQL_COLUMNS_TABLE_NAME, "start_time"
)

# Incremental record check
target_table_path = f"{DATABASE_NAME}.{SQL_COLUMNS_TABLE_NAME}"

original_statement_sql = f"SELECT statement_id, executed_by, start_time, end_time, statement_text FROM finops.system_query.history where statement_type = 'SELECT' AND 1=1 ORDER BY start_time ASC LIMIT {limit}"


# Create a schema for the parsed statement rows
parsed_sql_schema = StructType(
    [
        StructField("statement_id", StringType(), nullable=False),
        StructField("executed_by", StringType(), nullable=False),
        StructField("start_time", TimestampType(), nullable=False),
        StructField("end_time", TimestampType(), nullable=False),
        StructField("schema_name", StringType(), nullable=True),
        StructField("table_name", StringType(), nullable=True),
        StructField("column_name", StringType(), nullable=True),
    ]
)

if query_history_start_time is None:
    query_history_start_time = datetime.strptime("1970-01-01", "%Y-%m-%d")

while complete is False:
    query_statement_sql = original_statement_sql.replace(
        "1=1", f" start_time>'{query_history_start_time}'"
    )
    print(f"query_statement_sql: {query_statement_sql}")
    query_statements_df = spark.sql(query_statement_sql)
    recordCount = query_statements_df.count()
    print(f"start_time: {query_history_start_time}, records: {recordCount}")
    if recordCount < limit:
        complete = True

    query_statement_rows = query_statements_df.collect()

    parsed_statement_rows = []
    for row in query_statement_rows:
        statement_id = row["statement_id"]
        start_time = row["start_time"]
        end_time = row["end_time"]
        statement_text = row["statement_text"]
        executed_by = row["executed_by"]
        tokens = parse_sql(statement_text)
        for token in tokens:
            schema = str(token.getSchema())
            table = str(token.getTable())
            column = str(token.getColumn())
            # Create a row from statement_id, start_time, end_time, statement_text, schema, table, column
            parsed_row = (statement_id, executed_by,start_time, end_time, schema, table, column)
            # Add to parsed_statement_rows
            parsed_statement_rows.append(parsed_row)

        # increment start time
        query_history_start_time = start_time

    # Create a DataFrame from parsed_statement_rows
    parsed_df = spark.createDataFrame(parsed_statement_rows, parsed_sql_schema)
    parsed_df.show()
    if parsed_df.count()>0:
        parsed_df.dropDuplicates().write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(target_table_path)

