from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from kafka import KafkaProducer
import json

# Configuration Spark
spark = SparkSession.builder \
    .appName("WeatherStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des données reçues depuis Open-Meteo
schema = StructType([
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("generationtime_ms", FloatType(), True),
    StructField("current_weather", StructType([
        StructField("temperature", FloatType(), True),
        StructField("windspeed", FloatType(), True),
        StructField("winddirection", FloatType(), True),
        StructField("weathercode", FloatType(), True),
        StructField("time", StringType(), True)
    ]))
])

# Lire le flux depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Le message est en bytes, on le convertit en string
df = df.selectExpr("CAST(value AS STRING) as json_str")

# Parse le JSON
df_parsed = df.select(from_json(col("json_str"), schema).alias("data"))

# Extraire les colonnes utiles
df_weather = df_parsed.select(
    col("data.current_weather.temperature").alias("temperature"),
    col("data.current_weather.windspeed").alias("windspeed")
)

# Définir les fonctions d'alerte
def wind_alert(speed):
    if speed < 10:
        return "level_0"
    elif 10 <= speed <= 20:
        return "level_1"
    else:
        return "level_2"

def heat_alert(temp):
    if temp < 25:
        return "level_0"
    else:
        return "level_1"

# UDF Spark
from pyspark.sql.functions import udf
wind_udf = udf(wind_alert, StringType())
heat_udf = udf(heat_alert, StringType())

# Ajouter les colonnes d'alertes
df_alerts = df_weather.withColumn("wind_alert", wind_udf(col("windspeed"))) \
                      .withColumn("heat_alert", heat_udf(col("temperature")))

# Convertir en JSON pour Kafka
from pyspark.sql.functions import to_json, struct
df_out = df_alerts.select(to_json(struct(col("*"))).alias("value"))

# Écrire le flux transformé dans Kafka
query = df_out.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_transformed") \
    .option("checkpointLocation", "/tmp/checkpoint_weather") \
    .start()

query.awaitTermination()
