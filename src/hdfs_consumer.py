import sys
import json
from kafka import KafkaConsumer
from hdfs import InsecureClient

# Vérification arguments
if len(sys.argv) != 2:
    print("Usage: python hdfs_consumer.py <topic>")
    sys.exit(1)

topic = sys.argv[1]

# Connexion à HDFS
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')
# Connexion au broker Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:29092',  # port exposé pour l'extérieur
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)


print(f"📡 Listening to topic: {topic}")

for message in consumer:
    data = message.value
    country = data.get("country", "unknown")
    city = data.get("city", "unknown")

    # Construire le chemin HDFS
    hdfs_path = f"/hdfs-data/{country}/{city}/alerts.json"

    # Lire le fichier existant (si présent)
    try:
        with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
            existing_data = json.load(reader)
    except:
        existing_data = []

    # Ajouter le nouveau message
    existing_data.append(data)

    # Écrire le fichier HDFS
    with hdfs_client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
        json.dump(existing_data, writer, ensure_ascii=False, indent=2)

    print(f"✅ Message sauvegardé dans HDFS : {hdfs_path}")
