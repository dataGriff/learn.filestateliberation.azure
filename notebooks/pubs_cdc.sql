-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Pub Entity State Liberation

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Schema

-- COMMAND ----------

CREATE CATALOG beer;

-- COMMAND ----------

CREATE SCHEMA beer.pub;

-- COMMAND ----------

DROP TABLE IF EXISTS beer.pub.state;
CREATE TABLE IF NOT EXISTS beer.pub.state(
  pub STRING,
  number_of_beers INT,
  date_updated DATE
);
ALTER TABLE
  beer.pub.state
SET
  TBLPROPERTIES (delta.enableChangeDataCapture = true);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM beer.pub.state;

-- COMMAND ----------

-- MAGIC %md ## Source Data

-- COMMAND ----------

SELECT * FROM csv.`abfss://files@lrndatasaeundgrf.dfs.core.windows.net/pubs`;

-- COMMAND ----------

CREATE SCHEMA beer.files;

-- COMMAND ----------

DROP TABLE IF EXISTS beer.files.pub;
CREATE TABLE beer.files.pub (
  pub STRING,
  number_of_beers INT,
  date_updated DATE
) 
USING CSV
OPTIONS (HEADER 'TRUE')
LOCATION 'abfss://files@lrndatasaeundgrf.dfs.core.windows.net/pubs';

-- COMMAND ----------

select * from beer.files.pub

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Merge

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from delta.tables import DeltaTable
-- MAGIC schema = """pub STRING,
-- MAGIC   number_of_beers INT,
-- MAGIC   date_updated DATE"""
-- MAGIC
-- MAGIC # Read from the file source as a stream
-- MAGIC streamingDF = spark.readStream \
-- MAGIC     .format("csv") \
-- MAGIC     .option("header","true") \
-- MAGIC     .option("maxFilesPerTrigger", 1) \
-- MAGIC     .option("path", "abfss://files@lrndatasaeundgrf.dfs.core.windows.net/pubs") \
-- MAGIC     .schema(schema) \
-- MAGIC     .load()
-- MAGIC
-- MAGIC # Function to perform merge operation
-- MAGIC def merge_batch(batchDF, batchId):
-- MAGIC     # Assuming DeltaTable is already created and its path is known
-- MAGIC     deltaTable = DeltaTable.forName(spark,'beer.pub.state')
-- MAGIC
-- MAGIC     # Perform the merge
-- MAGIC     # Replace with your merge logic
-- MAGIC     deltaTable.alias("tgt").merge(
-- MAGIC         batchDF.alias("src"),
-- MAGIC         "tgt.pub = src.pub"
-- MAGIC     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
-- MAGIC
-- MAGIC # Apply the merge logic to each batch
-- MAGIC query = streamingDF.writeStream \
-- MAGIC     .foreachBatch(merge_batch) \
-- MAGIC     .outputMode("update") \
-- MAGIC     .trigger(availableNow=True) \
-- MAGIC     .start()

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Emit State Changes

-- COMMAND ----------

-- MAGIC %py
-- MAGIC change_data_stream = spark.readStream.format("delta") \
-- MAGIC                                      .option("readChangeData", "true") \
-- MAGIC                                      .table("beer.pub.state")
-- MAGIC
-- MAGIC # Apply the merge logic to each batch
-- MAGIC query = change_data_stream.filter("_change_type in ('insert','update_postimage')").writeStream \
-- MAGIC     .format("delta") \
-- MAGIC     .outputMode("append") \
-- MAGIC     .option("checkpointLocation", "lrndatasaeundgrf.dfs.core.windows.net/lake/pubs/history/checkpoint") \
-- MAGIC     .trigger(availableNow=True) \
-- MAGIC     .start("'lrndatasaeundgrf.dfs.core.windows.net/lake/pubs/history'")

-- COMMAND ----------

DROP TABLE IF EXISTS beer.pub.history;
CREATE TABLE beer.pub.history (
  pub STRING,
  number_of_beers INT,
  date_updated DATE
) 
LOCATION 'abfss://lake@lrndatasaeundgrf.dfs.core.windows.net/pubs/history';
