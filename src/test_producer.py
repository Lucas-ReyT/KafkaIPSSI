from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

test_message = {
    "temperature": 10.0,
    "windspeed": 12.0,
    "wind_alert": "level_1",
    "heat_alert": "level_0",
    "city": "Paris",
    "country": "France"
}

producer.send("weather_transformed", value=test_message)
producer.flush()
print("Message test envoyé")
