import os
os.environ['SPARK_HOME'] = "/Users/rohitsuresh/Downloads/spark-3.5.1-bin-hadoop3"
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaStreamReader") \
    .getOrCreate()

# Read data from Kafka topics
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "users_created") \
    .load()

# # Convert binary value to string
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Display the data on the console
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
