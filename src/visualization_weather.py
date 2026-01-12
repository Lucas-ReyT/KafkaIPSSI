# src/visualization_weather.py

import json
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1️⃣ Charger tous les fichiers JSON ---
files = glob.glob("src/hdfs_logs/*.json")
data_list = []

for f in files:
    with open(f, "r", encoding="utf-8") as file:
        data = json.load(file)
        data_list.extend(data)

df = pd.DataFrame(data_list)

# Vérifier les colonnes disponibles
print("Colonnes du DataFrame :", df.columns)
print(df.head())

# --- 2️⃣ Préparer les données ---
df['time'] = pd.to_datetime(df['time'])

# --- 3️⃣ Évolution de la température ---
plt.figure(figsize=(10,5))
sns.lineplot(data=df, x='time', y='temperature', hue='city', marker='o')
plt.title("Évolution de la température au fil du temps")
plt.xlabel("Temps")
plt.ylabel("Température (°C)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# --- 4️⃣ Évolution de la vitesse du vent ---
plt.figure(figsize=(10,5))
sns.lineplot(data=df, x='time', y='windspeed', hue='city', marker='o')
plt.title("Évolution de la vitesse du vent")
plt.xlabel("Temps")
plt.ylabel("Vitesse du vent (km/h)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# --- 5️⃣ Nombre d'alertes vent par niveau ---
alert_counts_wind = df.groupby(['city', 'wind_alert']).size().unstack(fill_value=0)
alert_counts_wind.plot(kind='bar', stacked=True, figsize=(10,5))
plt.title("Nombre d'alertes vent par niveau")
plt.ylabel("Nombre d'alertes")
plt.xlabel("Ville")
plt.tight_layout()
plt.show()

# --- 6️⃣ Nombre d'alertes chaleur par niveau ---
alert_counts_heat = df.groupby(['city', 'heat_alert']).size().unstack(fill_value=0)
alert_counts_heat.plot(kind='bar', stacked=True, figsize=(10,5))
plt.title("Nombre d'alertes chaleur par niveau")
plt.ylabel("Nombre d'alertes")
plt.xlabel("Ville")
plt.tight_layout()
plt.show()

# --- 7️⃣ Code météo le plus fréquent par pays ---
if 'weathercode' in df.columns:
    most_common_weather = df.groupby('country')['weathercode'].agg(lambda x: x.value_counts().idxmax())
    print("Code météo le plus fréquent par pays :")
    print(most_common_weather)
else:
    print("Aucune colonne 'weathercode' dans les fichiers, impossible de calculer le code météo le plus fréquent.")
