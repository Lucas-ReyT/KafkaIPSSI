from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, min, max, current_timestamp
from pyspark.sql.types import StructType, StructField, FloatType, StringType, TimestampType

# Initialisation Spark
spark = SparkSession.builder \
    .appName("WeatherAggregates") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des messages transformés
schema = StructType([
    StructField("temperature", FloatType(), True),
    StructField("windspeed", FloatType(), True),
    StructField("wind_alert", StringType(), True),
    StructField("heat_alert", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
])

# Lire depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_transformed") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON
df_parsed = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Ajouter un timestamp pour les fenêtres
df_parsed = df_parsed.withColumn("event_time", current_timestamp())

# 👇 Fenêtre glissante de 5 minutes, mise à jour toutes les 1 minute
windowed = df_parsed.groupBy(
    window(col("event_time"), "5 minutes", "1 minute"),
    col("wind_alert"),
    col("heat_alert")
).agg(
    count("*").alias("alert_count"),
    avg("temperature").alias("avg_temp"),
    min("temperature").alias("min_temp"),
    max("temperature").alias("max_temp")
)

# Affichage console pour debug
query = windowed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

