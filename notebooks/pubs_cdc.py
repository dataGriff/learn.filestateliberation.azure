# Databricks notebook source
# MAGIC %md 
# MAGIC # Pub Entity State Liberation

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Notes
# MAGIC * Installed maven lib on cluster - com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18
# MAGIC * Create event hub namespace with tls 1.2, kafka enabled and system assigned identity that had storage blob contributor on storage so can capture
# MAGIC * Created event hub with tombstone retention 

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

# MAGIC %md ## Event Hub

# COMMAND ----------

# DBTITLE 1,Display just to debug
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import lit, expr, current_timestamp, col, struct, to_json

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
             .withColumn("specversion", lit("1.0"))  \
             .withColumn("type", lit(eventType)) \
             .withColumn("time", col("_commit_timestamp")) \
             .withColumn("data", struct([df[col] for col in df.columns])) \
              .select(
                 col("pub").alias("partitionKey"),  # Assuming 'pub' is a column in your DataFrame
                 to_json(struct("id","time", "source", "specversion", "type", "data")).alias("body")
             )

source = "pubfiles/pubs"
eventType = "beer.pub.change"

change_data_stream1 = (spark.readStream.format("delta") \
                                     .option("startingVersion", "0") \
                                     .option("readChangeData", "true") \
                                     .table("beer.pub.state"))

cloudEventsDF = toCloudEvents(change_data_stream1.filter("_change_type in ('insert', 'update_postimage')"), source, eventType)

display(cloudEventsDF)


# COMMAND ----------

# DBTITLE 1,Hub Sorted - just add key when need to but should come from secrets, make sure find right place for checkpoint...
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import lit, expr, current_timestamp, col, struct, to_json

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
             .withColumn("specversion", lit("1.0"))  \
             .withColumn("type", lit(eventType)) \
             .withColumn("time", col("_commit_timestamp")) \
             .withColumn("data", struct([df[col] for col in df.columns])) \
              .select(
                 col("pub").alias("partitionKey"),  # Assuming 'pub' is a column in your DataFrame
                 to_json(struct("id","time", "source", "specversion", "type", "data")).alias("body")
             )

source = "pubfiles/pubs"
eventType = "beer.pub.change"

change_data_stream1 = (spark.readStream.format("delta") \
                                     .option("startingVersion", "0") \
                                     .option("readChangeData", "true") \
                                     .table("beer.pub.state"))

cloudEventsDF = toCloudEvents(change_data_stream1.filter("_change_type in ('insert', 'update_postimage')"), source, eventType)

##display(cloudEventsDF)

connectionString = "Endpoint=sb://lrn-datagriff-ehns-eun-dgrf.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey={key here};EntityPath=beer.cdc.pub.v3"

ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
}

# Assuming cloudEventsDF is your DataFrame ready to be written to Event Hubs
query = (cloudEventsDF
    .writeStream 
    .format("eventhubs") 
    .options(**ehConf) 
    .option("checkpointLocation", "abfss://lake@lrndatasaeundgrf.dfs.core.windows.net/pubs/ehv6/checkpoint") ## checkpoint! where should this go to be isolated?
    .outputMode("append") 
    .trigger(availableNow=True)
    .start())

# COMMAND ----------

# MAGIC %md ## Kafka Debacle

# COMMAND ----------

# from pyspark.sql.types import StructType, StringType, StructField
# from pyspark.sql.functions import lit, expr, current_timestamp, col, struct, to_json

# cloudEventSchema = StructType([
#     StructField("id", StringType(), False),
#     StructField("source", StringType(), False),
#     StructField("specversion", StringType(), False),
#     StructField("type", StringType(), False),
#     StructField("data", StringType(), True),
#     # Add other CloudEvents fields if needed
# ])

# def toCloudEvents(df, source, eventType):
#     return df.withColumn("id", expr("uuid()")) \
#              .withColumn("source", lit(source)) \
#              .withColumn("specversion", lit("1.0")) \
#              .withColumn("type", lit(eventType)) \
#              .withColumn("data", to_json(struct([df[col] for col in df.columns]))) \
#              .select(to_json(struct(
#                          col("data"),
#                          col("id"),
#                          col("source"),
#                          col("specversion"),
#                          col("type")
#                      )).alias("value"))

# source = "pubfiles/pubs"
# eventType = "beer.pub.change"

# change_data_stream1 = (spark.readStream.format("delta") \
#                                      .option("startingVersion", "0") \
#                                      .option("readChangeData", "true") \
#                                      .table("beer.pub.state"))

# cloudEventsDF = toCloudEvents(change_data_stream1.filter("_change_type in ('insert', 'update_postimage')"), source, eventType)

# topic_name = "beer.cdc.pub.v1"
# eh_namespace_name = "lrn-datagriff-ehns-eun-dgrf"
# connectionString = "Endpoint=sb://lrn-datagriff-ehns-eun-dgrf.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=="
# eh_sasl = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule' \
#     + f' required username="$ConnectionString" password="{connectionString}";'
# bootstrap_servers = f"{eh_namespace_name}.servicebus.windows.net:9093"

# kafka_options = {
#      "kafka.bootstrap.servers": bootstrap_servers,
#      "kafka.sasl.mechanism": "PLAIN",
#      "kafka.security.protocol": "SASL_SSL",
#      "kafka.sasl.jaas.config": eh_sasl,
#      "topic": topic_name,
# }

# (cloudEventsDF.select(to_json(struct("*")).alias("value"))
#   .writeStream.format("kafka")
#     .options(**kafka_options)
#     .option("checkpointLocation", "abfss://lake@lrndatasaeundgrf.dfs.core.windows.net/pubs/kafka/checkpoint") 
#     .outputMode("append") 
#     .trigger(availableNow=True) \
#     .start())

# ##display(cloudEventsDF)

# COMMAND ----------

# from pyspark.sql.types import StructType, StringType, StructField
# from pyspark.sql.functions import lit, expr, current_timestamp, col, struct, to_json

# cloudEventSchema = StructType([
#     StructField("id", StringType(), False),
#     StructField("source", StringType(), False),
#     StructField("specversion", StringType(), False),
#     StructField("type", StringType(), False),
#     StructField("data", StringType(), True),
#     # Add other CloudEvents fields if needed
# ])

# def toCloudEvents(df, source, eventType):
#     return df.withColumn("id", expr("uuid()")) \
#              .withColumn("source", lit(source)) \
#              .withColumn("specversion", lit("1.0")) \
#              .withColumn("type", lit(eventType)) \
#              .withColumn("data", to_json(struct([df[col] for col in df.columns]))) \
#              .select(to_json(struct(
#                          col("data"),
#                          col("id"),
#                          col("source"),
#                          col("specversion"),
#                          col("type")
#                      )).alias("value"))

# source = "pubfiles/pubs"
# eventType = "beer.pub.change"

# change_data_stream1 = (spark.readStream.format("delta") \
#                                      .option("startingVersion", "0") \
#                                      .option("readChangeData", "true") \
#                                      .table("beer.pub.state"))

# cloudEventsDF = toCloudEvents(change_data_stream1.filter("_change_type in ('insert', 'update_postimage')"), source, eventType)

# topic_name = "beer.cdc.pub.v1"
# eh_namespace_name = "lrn-datagriff-ehns-eun-dgrf"
# connectionString = "="
# eh_sasl = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule' \
#     + f' required username="$ConnectionString" password="{connectionString}";'
# bootstrap_servers = f"{eh_namespace_name}.servicebus.windows.net:9093"

# kafka_options = {
#      "kafka.bootstrap.servers": bootstrap_servers,
#      "kafka.sasl.mechanism": "PLAIN",
#      "kafka.security.protocol": "SASL_SSL",
#      "kafka.sasl.jaas.config": eh_sasl,
#      "topic": topic_name,
# }

# (cloudEventsDF.select(to_json(struct("*")).alias("value"))
#   .writeStream.format("kafka")
#     .options(**kafka_options)
#     .option("checkpointLocation", "abfss://lake@lrndatasaeundgrf.dfs.core.windows.net/pubs/kafka2/checkpoint") 
#     .outputMode("append") 
#     .trigger(availableNow=True) \
#     .start())

# ##display(cloudEventsDF)

# COMMAND ----------

# DBTITLE 1,kafka carnage
# # query = cloudEventsDF \
# #     .writeStream \
# #     .format("kafka") \
# #     .option("kafka.bootstrap.servers", "lrn-datagriff-ehns-eun-dgrf.servicebus.windows.net") \
# #     .option("kafka.security.protocol", "SASL_SSL") \
# #     .option("kafka.sasl.mechanism", "PLAIN") \
# #     .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"<<<EVENTHUBS SASL PLAIN CONNECTION STRING>>>\";") \
# #     .option("topic", "beer.cdc.pub.v1") \
# #     .start()

# ehKafkaConnectionString = "lrn-datagriff-ehns-eun-dgrf.servicebus.windows.net:9093"  # Replace with your namespace 9093
# ehName = "beer.cdc.pub.v1"  # Replace with your Event Hub name
# connectionString = "{0};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=0KW4taBAqbIxh+sMLgSX1aZ/2JX22sd1o+AEhCQyHGw=".format(ehKafkaConnectionString)  # Replace with your connection string details

# # Kafka related settings
# kafkaWriteConfig = {
#   "kafka.sasl.mechanism": "PLAIN",
#   "kafka.security.protocol": "SASL_SSL",
#   "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{}";'.format(connectionString),
#   "kafka.bootstrap.servers": ehKafkaConnectionString,
#   "topic": ehName
# }

# query = cloudEventsDF.writeStream \
#     .format("kafka") \
#     .options(**kafkaWriteConfig) \
#     .option("checkpointLocation", "abfss://lake@lrndatasaeundgrf.dfs.core.windows.net/pubs/kafka/checkpoint") \
#     .outputMode("append") \
#     .start()

#     topic_name = "beer.cdc.pub.v1"
# eh_namespace_name = "lrn-datagriff-ehns-eun-dgrf"
# connectionString = "Endpoint=sb://lrn-datagriff-ehns-eun-dgrf.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=0KW4taBAqbIxh+sMLgSX1aZ/2JX22sd1o+AEhCQyHGw="
# eh_sasl = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule' \
#     + f' required username="$ConnectionString" password="{connectionString}";'
# bootstrap_servers = f"{eh_namespace_name}.servicebus.windows.net:9093"

# kafka_options = {
#      "kafka.bootstrap.servers": bootstrap_servers,
#      "kafka.sasl.mechanism": "PLAIN",
#      "kafka.security.protocol": "SASL_SSL",
#      "kafka.sasl.jaas.config": eh_sasl,
#      "topic": topic_name,
# }
# cloudEventsDF.select(to_json(struct("*")).alias("value")) \
#   .writeStream.format("kafka").options(**kafka_options).trigger(availableNow=True).option("checkpointLocation", "abfss://lake@lrndatasaeundgrf.dfs.core.windows.net/pubs/kafka/checkpoint") \
#     .outputMode("append") \
#     .start()
