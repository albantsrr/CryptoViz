from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, stddev
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("BitcoinVolatilityAnalysis") \
    .master("spark://83.159.114.67:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "5.135.156.86:9092") \
    .option("subscribe", "bitcoin_topic") \
    .load()

df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

df = df.withColumn("value", col("value").cast(DoubleType()))

# Calcul de l'écart-type sur 1 heure
volatility = df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), "1 hour")) \
    .agg(stddev("value").alias("price_volatility"))

volatility = volatility.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("price_volatility")
)

# Écriture dans Cassandra
def writeToCassandra(batch_df, epoch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
	.option("spark.cassandra.connection.host", "83.159.114.67") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace", "bitcoin_data") \
        .option("table", "bitcoin_volatility") \
        .save()
    pass

query = volatility.writeStream \
    .foreachBatch(writeToCassandra) \
    .outputMode("update") \
    .start()

query.awaitTermination()
