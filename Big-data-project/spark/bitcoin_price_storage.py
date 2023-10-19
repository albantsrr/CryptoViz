from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import FloatType, TimestampType

spark = SparkSession.builder \
    .appName("BitcoinPriceStorage") \
    .master("spark://0.0.0.0:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "24g") \
    .config("spark.executor.cores", "16") \
    .config("spark.worker.timeout", "240000") \
    .getOrCreate()
#    
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "5.135.156.86:9092") \
    .option("subscribe", "bitcoin_topic") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

# df = df.withColumn("value", df["value"].cast(FloatType()))
# df = df.withColumn("timestamp", df["timestamp"].cast(TimestampType()))
# df = df.withColumn("timestamp", unix_timestamp(df["timestamp"], "yyyy-MM-dd HH:mm:ss.SSS").cast("timestamp"))

query = df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", True) \
    .start()

query.awaitTermination()

