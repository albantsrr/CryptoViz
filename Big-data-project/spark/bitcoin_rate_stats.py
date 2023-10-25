from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, expr, current_timestamp, round
from pyspark.sql.types import DoubleType, TimestampType
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("BitcoinRateStats") \
    .master("spark://86.201.248.56:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "5.135.156.86:9092") \
    .option("subscribe", "bitcoin_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Sélection des collums souhaitées pour le traitement
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

# Pré-Traitement des collumns pour utilisation (Conversion de type etc...)  
df = df.withColumn("value", round(df["value"].cast(DoubleType()), 2))
df = df.withColumn("timestamp", df["timestamp"].cast(TimestampType()))
df = df.withColumn("timestamp", unix_timestamp(df["timestamp"], "yyyy-MM-dd HH:mm:ss.SSS").cast("timestamp"))

# Agrégation des données sur 24 heures / 1 minutes / 2 heures / ...
aggregated_df = df.groupBy("key", F.window("timestamp", "24 hours", "24 hours")).agg(
    F.first("value").alias("start_value"),
    F.last("value").alias("latest_value")
)

aggregated_df = aggregated_df.withColumn("calculation_time", current_timestamp()) 

# Calcule du taux de difference du cours du bitcoin sur 24H
aggregated_df = aggregated_df.withColumn("difference_rate", 
    ((col("latest_value") - col("start_value")))
)

# Calcule du taux de variation du cours du bitcoin sur 24H
result_df = aggregated_df.withColumn("variation_rate", 
    ((col("latest_value") - col("start_value")) / col("start_value")) * 100
)

result_df = result_df.withColumn("win_date_start", expr("window.start"))
result_df = result_df.withColumn("win_date_end", expr("window.end"))

columns_to_insert = ["key", "win_date_start", "win_date_end", "start_value", "latest_value", "calculation_time", "difference_rate", "variation_rate"]

result_df = result_df.select(columns_to_insert)

def updateCassandra(batch_df, batch_id):

    selected_df = batch_df.select("win_date_start", "win_date_end", "start_value", "latest_value", "calculation_time", "difference_rate", "variation_rate")

    selected_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("overwrite") \
        .option("confirm.truncate", True) \
        .option("spark.cassandra.output.batch.size.bytes", "1024") \
        .option("spark.cassandra.output.concurrent.writes", 2) \
        .option("spark.cassandra.connection.host", "5.135.156.86") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(table="bitcoin_stats", keyspace="bitcoin_data") \
        .save()


query = result_df.writeStream \
    .outputMode("update") \
    .foreachBatch(updateCassandra) \
    .start()

query.awaitTermination()
