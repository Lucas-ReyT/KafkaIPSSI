import sys
import json
import time
import requests
from kafka import KafkaProducer

# Vérification des arguments
if len(sys.argv) != 3:
    print("Usage: python current_weather.py <latitude> <longitude>")
    sys.exit(1)

latitude = sys.argv[1]
longitude = sys.argv[2]

# Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',  # ou 'kafka:9092' si lancé dans un container Docker
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

topic = "weather_stream"

print(f"📡 Envoi des données météo pour lat={latitude}, lon={longitude} vers topic '{topic}'")

# Boucle infinie pour envoyer les données toutes les X secondes
try:
    while True:
        # Requête Open-Meteo
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        response = requests.get(url)
        if response.status_code == 200:
            weather_data = response.json()
            # Envoi vers Kafka
            producer.send(topic, value=weather_data)
            print("✅ Message envoyé :", weather_data)
        else:
            print("⚠️ Erreur API :", response.status_code)

        time.sleep(10)  # attendre 10 secondes avant la prochaine requête
except KeyboardInterrupt:
    print("⏹ Arrêt du producteur")
finally:
    producer.close()
