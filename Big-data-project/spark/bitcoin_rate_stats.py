from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, expr, current_timestamp, round, from_unixtime
from pyspark.sql.types import DoubleType, TimestampType
from pyspark.sql import functions as F
import json
from datetime import datetime
# Récupération de l'offset sur la table (bitcoin_latest_stats_offset_treated).

spark = SparkSession.builder \
    .appName("BitcoinRateStats") \
    .master("spark://0.0.0.0:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .getOrCreate()

startingOffsets = {
   "bitcoin_topic": {
       0: 380000
   }
}

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "5.135.156.86:9092") \
    .option("subscribe", "bitcoin_topic") \
    .option("startingOffsets", json.dumps(startingOffsets)) \
    .load()

# Sélection des collums souhaitées pour le traitement
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)", "CAST(offset AS INTEGER)")

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

PREVIOUS_BATCH_DF = None

def is_changed_day(current_batch_df, previous_batch_df):
    # Logique pour savoir par rapport au current_batch_df et previous_batch_df si ont va passer au jour d'après
    # Je récupére la win_date_start et win_date_end et je regarde sur les deux batch si les date correspondent.
    # Si jour d'après, return True
    # Si toujours jour actuelle
    current_win_date_start = current_batch_df.win_date_start
    current_win_date_end = current_batch_df.win_date_end
    previous_win_date_start = previous_batch_df.win_date_start
    previous_win_date_end = previous_batch_df.win_date_end
    print(current_win_date_start, current_win_date_end, previous_win_date_start, previous_win_date_end)
    return True 

def process_batch_zero(batch_df):
    # Logique pour traiter le batch 0 
    # Si plusieur ligne, je filtre la ligne qui correspond a la tranche 24h du jour actuelle
    # Puis j'insére la données dans (bitcoin_stats_history)
    actual_timestamp = datetime.now()
    current_day = actual_timestamp.date()
    filtered_batch_df = batch_df.filter(batch_df["win_date_start"].cast("date") != current_day)

    filtered_batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("confirm.truncate", True) \
        .option("spark.cassandra.output.batch.size.bytes", "1024") \
        .option("spark.cassandra.output.concurrent.writes", 2) \
        .option("spark.cassandra.connection.host", "5.135.156.86") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(table="bitcoin_stats_history", keyspace="bitcoin_data") \
        .save()

def process_batch(batch_df, batch_id):
    
    if batch_id == 0:
        print("je passe dans le batch 0 process")
        bitcoin_stats_history = batch_df.select("win_date_start", "win_date_end", "start_value", "latest_value", "difference_rate", "variation_rate")
        process_batch_zero(bitcoin_stats_history)
    else:
        
        bitcoin_stats_refreshed = batch_df.select("win_date_start", "win_date_end", "start_value", "latest_value", "calculation_time", "difference_rate", "variation_rate")
        #bitcoin_latest_stats_offset_treated = batch_df.select("timestamp","offset")
        
        #is_changed_day(bitcoin_stats_refreshed, PREVIOUS_BATCH_DF)

        bitcoin_stats_refreshed.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .option("confirm.truncate", True) \
            .option("spark.cassandra.output.batch.size.bytes", "1024") \
            .option("spark.cassandra.output.concurrent.writes", 2) \
            .option("spark.cassandra.connection.host", "5.135.156.86") \
            .option("spark.cassandra.connection.port", "9042") \
            .options(table="bitcoin_stats_refreshed", keyspace="bitcoin_data") \
            .save()
        
        # bitcoin_latest_stats_offset_treated.write \
        #     .format("org.apache.spark.sql.cassandra") \
        #     .mode("overwrite") \
        #     .option("confirm.truncate", True) \
        #     .option("spark.cassandra.output.batch.size.bytes", "1024") \
        #     .option("spark.cassandra.output.concurrent.writes", 2) \
        #     .option("spark.cassandra.connection.host", "5.135.156.86") \
        #     .option("spark.cassandra.connection.port", "9042") \
        #     .options(table="bitcoin_latest_stats_offset_treated", keyspace="bitcoin_data") \
        #     .save()


query = result_df.writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
