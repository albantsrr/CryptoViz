from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.functions import unix_timestamp, round
from pyspark.sql.types import DoubleType, TimestampType
import json

cluster = Cluster(['172.19.0.17'], port=9042) 
session = cluster.connect()

session.set_keyspace('bitcoin_data') 

query = "SELECT offset FROM bitcoin_latest_offset_treated;"
result = session.execute(query)
last_offset_treated = result.one()
print(last_offset_treated)
session.shutdown()
cluster.shutdown()

spark = SparkSession.builder \
    .appName("BitcoinPriceStorage") \
    .master("spark://83.159.114.67:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.network.timeout", "240000") \
    .getOrCreate()

startingOffsets = {
   "bitcoin_topic": {
       0: last_offset_treated[0] if last_offset_treated is not None else 380000
   }
}

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "5.135.156.86:9092") \
    .option("subscribe", "bitcoin_topic") \
    .option("startingOffsets", json.dumps(startingOffsets)) \
    .option("groupId", "store_bitcoin_data") \
    .option("failOnDataLoss", "false") \
    .load()


df = df.selectExpr("CAST(key AS STRING)", "CAST(offset AS INTEGER)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

df = df.withColumn("price", round(df["value"].cast(DoubleType()), 2))
df = df.withColumn("timestamp", df["timestamp"].cast(TimestampType()))
df = df.withColumn("timestamp", unix_timestamp(df["timestamp"], "yyyy-MM-dd HH:mm:ss.SSS").cast("timestamp"))

print(df); 

def process_batch(batch_df, batch_id):
    selected_df_for_bitcoin_prices = batch_df.select("timestamp", "price")
    selected_df_for_bitcoin_offset_latest = batch_df.select("offset", "timestamp")

    selected_df_for_bitcoin_prices.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("confirm.truncate", True) \
        .option("spark.cassandra.output.batch.size.bytes", "1024") \
        .option("spark.cassandra.output.concurrent.writes", 2) \
        .option("spark.cassandra.connection.host", "172.19.0.17") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(table="bitcoin_prices", keyspace="bitcoin_data") \
        .save()
    
    selected_df_for_bitcoin_offset_latest.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("overwrite") \
        .option("confirm.truncate", True) \
        .option("spark.cassandra.output.batch.size.bytes", "1024") \
        .option("spark.cassandra.output.concurrent.writes", 2) \
        .option("spark.cassandra.connection.host", "172.19.0.17") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(table="bitcoin_latest_offset_treated", keyspace="bitcoin_data") \
        .save()

query = df.writeStream \
    .outputMode("update") \
    .format("console") \
    .foreachBatch(process_batch) \
    .option("truncate", False) \
    .start()

query.awaitTermination()

