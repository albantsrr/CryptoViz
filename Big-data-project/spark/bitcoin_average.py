from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col, avg, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType
# Créez une session Spark
#.master("spark://0.0.0.0:7077") \
spark = SparkSession.builder \
    .appName("BitcoinAverageStreamingKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

#spark.sparkContext.setLogLevel("DEBUG")

# Créez un schéma pour le désérialiseur JSON
schema = StructType([StructField("key", StringType(), True), StructField("value", FloatType(), True)])

# Chargez les données depuis Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.19.0.3:9092") \
    .option("subscribe", "bitcoin_topic") \
    .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Convertissez la colonne "value" en float
df = df.withColumn("value", df["value"].cast(FloatType()))

# Calculez la moyenne du cours du Bitcoin en utilisant Spark SQL
average_price = df.groupBy().avg("value")

print(average_price)
# Affichez la moyenne en streaming (mode append)
query = average_price.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

print(query)

# Attendez la fin de l'application
query.awaitTermination()

