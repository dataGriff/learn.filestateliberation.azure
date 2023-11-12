# Databricks notebook source
# MAGIC %md 
# MAGIC # Pub Entity State Liberation

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS beer;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS beer.pub;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS beer.pub.state;
# MAGIC CREATE TABLE IF NOT EXISTS beer.pub.state(
# MAGIC   pub STRING,
# MAGIC   number_of_beers INT,
# MAGIC   date_updated DATE
# MAGIC )
# MAGIC USING delta
# MAGIC OPTIONS ('delta.enableChangeDataCapture'='true');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY beer.pub.state;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM beer.pub.state;

# COMMAND ----------

# MAGIC %md ## Source Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`abfss://files@lrndatasaeundgrf.dfs.core.windows.net/pubs`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS beer.files;

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS beer.files.pub;
# MAGIC CREATE TABLE IF NOT EXISTS beer.files.pub (
# MAGIC   pub STRING,
# MAGIC   number_of_beers INT,
# MAGIC   date_updated DATE
# MAGIC ) 
# MAGIC USING CSV
# MAGIC OPTIONS (HEADER 'TRUE')
# MAGIC LOCATION 'abfss://files@lrndatasaeundgrf.dfs.core.windows.net/pubs';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from beer.files.pub

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Merge

# COMMAND ----------

# MAGIC %py
# MAGIC from delta.tables import DeltaTable
# MAGIC schema = """pub STRING,
# MAGIC   number_of_beers INT,
# MAGIC   date_updated DATE"""
# MAGIC
# MAGIC # Read from the file source as a stream
# MAGIC streamingDF = spark.readStream \
# MAGIC     .format("csv") \
# MAGIC     .option("header","true") \
# MAGIC     .option("maxFilesPerTrigger", 1) \
# MAGIC     .option("path", "abfss://files@lrndatasaeundgrf.dfs.core.windows.net/pubs") \
# MAGIC     .schema(schema) \
# MAGIC     .load()
# MAGIC
# MAGIC # Function to perform merge operation
# MAGIC def merge_batch(batchDF, batchId):
# MAGIC     # Assuming DeltaTable is already created and its path is known
# MAGIC     deltaTable = DeltaTable.forName(spark,'beer.pub.state')
# MAGIC
# MAGIC     # Perform the merge
# MAGIC     # Replace with your merge logic
# MAGIC     deltaTable.alias("tgt").merge(
# MAGIC         batchDF.alias("src"),
# MAGIC         "tgt.pub = src.pub"
# MAGIC     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
# MAGIC
# MAGIC # Apply the merge logic to each batch
# MAGIC query = streamingDF.writeStream \
# MAGIC     .foreachBatch(merge_batch) \
# MAGIC     .outputMode("update") \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM beer.pub.state;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Emit State Changes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS beer.pub.history;
# MAGIC CREATE TABLE IF NOT EXISTS beer.pub.history (
# MAGIC   pub STRING,
# MAGIC   number_of_beers INT,
# MAGIC   date_updated DATE,
# MAGIC   _change_type STRING,
# MAGIC   _commit_version LONG,
# MAGIC   _commit_timestamp TIMESTAMP
# MAGIC ) 
# MAGIC LOCATION 'abfss://lake@lrndatasaeundgrf.dfs.core.windows.net/pubs/history';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY beer.pub.state;

# COMMAND ----------

# MAGIC %python
# MAGIC change_data_stream1 = (spark.readStream.format("delta") \
# MAGIC                                     .option("startingVersion", "2") \
# MAGIC                                      .option("readChangeData", "true") \
# MAGIC                                      .table("beer.pub.state"))
# MAGIC
# MAGIC query = change_data_stream1.filter("_change_type in ('insert', 'update_postimage')").writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "abfss://lake@lrndatasaeundgrf.dfs.core.windows.net/pubs/history/checkpoint") \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .start("abfss://lake@lrndatasaeundgrf.dfs.core.windows.net/pubs/history")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM beer.pub.history

# COMMAND ----------

pip install --upgrade pip

# COMMAND ----------

pip install cloudevents

# COMMAND ----------

!pip install spark-structured-streaming-with-azure-event-hubs

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import lit, to_json, struct, window, current_timestamp

cloudEventSchema = StructType([
    StructField("id", StringType(), False),
    StructField("source", StringType(), False),
    StructField("specversion", StringType(), False),
    StructField("type", StringType(), False),
    StructField("data", StringType(), True),
    # Add other CloudEvents fields if needed
])

def toCloudEvents(df, source, eventType):
    return df.withColumn("id", expr("uuid()")) \
             .withColumn("source", lit(source)) \
             .withColumn("specversion", lit("1.0")) \
             .withColumn("type", lit(eventType)) \
             .withColumn("data", to_json(struct([df[col] for col in df.columns]))) \
             .select([col for col in cloudEventSchema.names])

source = "pubfiles/pubs"
eventType = "beer.pub.change"

change_data_stream1 = (spark.readStream.format("delta") \
                                     .option("startingVersion", "0") \
                                     .option("readChangeData", "true") \
                                     .table("beer.pub.state"))

cloudEventsDF = toCloudEvents(change_data_stream1.filter("_change_type in ('insert', 'update_postimage')"), source, eventType)

display(cloudEventsDF)
