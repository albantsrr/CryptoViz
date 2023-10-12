from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col, unix_timestamp, first, last, window, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("BitcoinRateVariation") \
    .master("spark://5.135.156.86:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

schema = StructType([StructField("key", StringType(), True), StructField("value", FloatType(), True)])

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
df = df.withColumn("value", df["value"].cast(FloatType()))
df = df.withColumn("timestamp", df["timestamp"].cast(TimestampType()))
df = df.withColumn("timestamp", unix_timestamp(df["timestamp"], "yyyy-MM-dd HH:mm:ss.SSS").cast("timestamp"))

# Agrégation des données sur 24 heures / 1 minutes / 2 heures / ...
aggregated_df = df.groupBy("key", F.window("timestamp", "12 hours", "12 hours")).agg(
    F.first("value").alias("start_value"),
    F.last("value").alias("latest_value")
)

# Calculez le difference de variation
result_df = aggregated_df.withColumn("rate_variation", 
    ((col("latest_value") - col("start_value")) / col("start_value")) * 100
)

# Ecriture sur console de la difference de variation en temps réel
query = result_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()