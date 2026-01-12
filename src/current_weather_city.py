import sys
import json
import time
import requests
from kafka import KafkaProducer

# Vérification des arguments
if len(sys.argv) != 3:
    print("Usage: python current_weather_city.py <city> <country>")
    sys.exit(1)

city = sys.argv[1]
country = sys.argv[2]

# --- Étape 1 : Géocodage pour récupérer latitude/longitude ---
geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count=1&language=en&format=json"
response = requests.get(geo_url)

if response.status_code != 200 or "results" not in response.json():
    print(f"❌ Impossible de récupérer les coordonnées pour {city}, {country}")
    sys.exit(1)

geo_data = response.json()["results"][0]
latitude = geo_data["latitude"]
longitude = geo_data["longitude"]

print(f"📍 Coordonnées pour {city}, {country} : lat={latitude}, lon={longitude}")

# --- Étape 2 : Connexion à Kafka ---
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',  # ou 'kafka:9092' si exécuté dans un container Docker
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

topic = "weather_stream"

# --- Étape 3 : Boucle pour envoyer les données météo ---
try:
    while True:
        weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        response = requests.get(weather_url)
        if response.status_code == 200:
            weather_data = response.json()

            # Ajouter ville et pays dans le message
            weather_data["city"] = city
            weather_data["country"] = country

            producer.send(topic, value=weather_data)
            print("✅ Message envoyé :", weather_data)
        else:
            print("⚠️ Erreur API météo :", response.status_code)

        time.sleep(10)  # attendre 10 secondes avant la prochaine requête

except KeyboardInterrupt:
    print("⏹ Arrêt du producteur")

finally:
    producer.close()
