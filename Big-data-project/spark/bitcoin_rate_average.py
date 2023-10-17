from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col, avg, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder \
    .appName("BitcoinRateAverage") \
    .master("spark://0.0.0.0:7077") \
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

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df = df.withColumn("value", df["value"].cast(FloatType()))

average_price = df.filter(df["key"] == "btc_key").groupBy().avg("value")

query = average_price.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

