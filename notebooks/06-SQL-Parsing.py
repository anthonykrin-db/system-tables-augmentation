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
# MAGIC /Volumes/finops/system_lookups/libs/sql_parser_excl_gudu_v1.3.jar```
# MAGIC

# COMMAND ----------

from py4j.java_gateway import java_import
from pyspark.sql.functions import udf, col
from datetime import datetime
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DateType,TimestampType

java_import(spark._jvm, "parser.ColumnToken") 
java_import(spark._jvm, "parser.ParseResult") 
java_import(spark._jvm, "parser.SqlStatementParser")

parser = spark._jvm.SqlStatementParser()

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
            print(f"Unable to parse {sql}: {result}")
    except Exception as e:
        print(f"Exception parsing {sql}: {e}")
    return tokens

def parse_sql_statements(sqlList):
    results = []
    if len(sqlList)==0:
        return results
    try:
        multipleResults = parser.parseMultipleTokens(sqlList)
        for result in multipleResults:
            if result.isSuccess():
                #print("Success: " + str(result.isSuccess()))
                results.append(result.getTokens())
            else:
                print(f"Unable to parse {sql}: {result.getErrMsg()}")
                continue
    except Exception as e:
        print(f"Exception parsing {sql}: {e}")
    return results

def printToken(token):
    print("===== TOKEN =====\n")
    print("Catalog: " + str(token.getCatalog())) 
    print("Schema: " + str(token.getSchema())) 
    print("Table: " + str(token.getTable())) 
    print("Column: " + str(token.getColumn())) 
    print("-------------\n")

# COMMAND ----------

# DBTITLE 1,Parsing Tests


sql = "with y as (SELECT * FROM x.tbl_a) select a,b,c,DATETOSTRING(date_attr_name,'mm/dd/yyyy') from y"
sql = "select cs.DeviceId,cs.MessageDateTime,cs.HeartRate, cs.RespiratoryRate,cs.BloodPressureSystolic, cs.BloodPressureDiastolic,concat_ws('\\\\',string(int(cs.BloodPressureSystolic)),string(int(cs.BloodPressureDiastolic))) as BloodPressure, cs.BodyTemperature, cs.HeartRateVariability from iomt_demo.charts_silver cs where cs.DeviceId = '1qsi9p8t5l2'  and cs.HeartRate is not null  and cs.MessageDateHour = date_format(current_timestamp(), 'yyyy-MM-dd HH:00:00') order by cs.MessageDateTime desc LIMIT 1000"

tokens = parse_sql(sql)
for token in tokens: 
  printToken(token)

print("**************** Multiple ****************")

sqlList = []
sqlList.append(sql)
sqlList.append(sql)
sqlList.append(sql)
sqlList.append(sql)
results = parse_sql_statements(sqlList)

for result in results:
  for token in result:
    printToken(token)



# COMMAND ----------

# DBTITLE 1,Parse query history

import time
import datetime

start_time = time.time()

statement_batch_size = 1000
complete = False

# SQL_COLUMNS_TABLE_NAME
query_history_start_time = lookup_last_record_value(
    DATABASE_NAME, SQL_COLUMNS_TABLE_NAME, "start_time"
)

# Incremental record check
target_table_path = f"{DATABASE_NAME}.{SQL_COLUMNS_TABLE_NAME}"

original_statement_sql = f"SELECT statement_id, executed_by, start_time, end_time, statement_text FROM finops.system_query.history where statement_type = 'SELECT' AND 1=1 ORDER BY start_time ASC LIMIT {statement_batch_size}"

# Create a schema for the parsed statement rows
parsed_sql_schema = StructType(
    [
        StructField("statement_id", StringType(), nullable=False),
        StructField("executed_by", StringType(), nullable=False),
        StructField("start_time", TimestampType(), nullable=False),
        StructField("end_time", TimestampType(), nullable=False),
        StructField("catalog_name", StringType(), nullable=True),
        StructField("schema_name", StringType(), nullable=True),
        StructField("table_name", StringType(), nullable=True),
        StructField("column_name", StringType(), nullable=True),
    ]
)

if query_history_start_time is None:
    query_history_start_time = datetime.strptime("1970-01-01", "%Y-%m-%d")

batch = 0
while complete is False:
    loop_start = time.time()
    batch = batch +1
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

    sql_statements = []
    statement_metadata = []

    for row in query_statement_rows:
        
        sql = row["statement_text"]
        print(f"Parsing sql: {sql}")
        sql_statements.append(row["statement_text"])
        statement_metadata.append({
            "statement_id": row["statement_id"],
            "executed_by": row["executed_by"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "statement_text": row["statement_text"]
        })

        # Update query_history_start_time
        query_history_start_time = row["start_time"]

    # Parse all SQL statements at once
    all_tokens = parse_sql_statements(sql_statements)

    parsed_statement_rows = []
    for tokens, metadata in zip(all_tokens, statement_metadata):
        for token in tokens:
            catalog = str(token.getCatalog())
            schema = str(token.getSchema())
            table = str(token.getTable())
            column = str(token.getColumn())
            
            parsed_row = (
                metadata["statement_id"],
                metadata["executed_by"],
                metadata["start_time"],
                metadata["end_time"],
                catalog,
                schema,
                table,
                column
            )
            
            parsed_statement_rows.append(parsed_row)

    # Create a DataFrame from parsed_statement_rows
    parsed_df = spark.createDataFrame(parsed_statement_rows, parsed_sql_schema)
    #parsed_df.show()
    if parsed_df.count()>0:
        parsed_df.dropDuplicates().write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(target_table_path)
        loop_end = time.time()
        loop_duration = loop_end - loop_start
        total_elapsed = loop_end - start_time
        print(f"Batch {batch} duration: {loop_duration:.2f} seconds. Total time elapsed: {total_elapsed:.2f} seconds")
        print(f"Parsed {statement_batch_size} SQL statements.  Committed {parsed_df.count()} parsed cat/schema/table/column records.  Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
   

