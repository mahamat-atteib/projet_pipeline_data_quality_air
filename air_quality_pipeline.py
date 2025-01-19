import requests
import pandas as pd
from flask import Flask, jsonify
from google.cloud import bigquery
import datetime

# API OpenAQ URL de base
BASE_URL = "https://api.openaq.org/v2/measurements"
PARAMETERS = ["pm25", "pm10", "so2", "o3", "co", "bc", "no2"]

# Liste des capitales africaines
AFRICAN_CAPITALS = [
    {"city": "Algiers", "country": "DZ"},
    {"city": "Luanda", "country": "AO"},
    # Ajoutez les autres capitales ici...
]

# Flask app
app = Flask(__name__)

# Fonction pour récupérer les données de l'API OpenAQ
def fetch_air_quality_data(city, country, parameters, date_from, limit=100):
    all_results = []
    for parameter in parameters:
        params = {
            "city": city,
            "country": country,
            "parameter": parameter,
            "date_from": date_from,
            "limit": limit
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            if "results" in data:
                all_results.extend(data["results"])
        else:
            print(f"Erreur pour {city}, {parameter}: {response.status_code}, {response.text}")
    return all_results

# Collecter les données des capitales africaines
def collect_african_air_quality(last_execution_time):
    all_data = []
    for capital in AFRICAN_CAPITALS:
        city = capital["city"]
        country = capital["country"]
        print(f"Collecte des données pour {city}, {country}...")
        city_data = fetch_air_quality_data(city, country, PARAMETERS, last_execution_time)
        all_data.extend(city_data)
    return all_data

# Transformer les données
def transform_air_quality_data(data):
    records = []
    for item in data:
        record = {
            "id": item["id"],  # ID unique pour éviter les doublons
            "city": item["city"],
            "country": item["country"],
            "parameter": item["parameter"],
            "value": item["value"],
            "unit": item["unit"],
            "date": item["date"]["utc"]
        }
        records.append(record)
    return pd.DataFrame(records)

# Charger les données dans BigQuery
def load_to_bigquery(df, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Configurer l'option WRITE_TRUNCATE pour écraser les anciennes données
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1
    )

    # Charger les données
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Attendre la fin du job
    print(f"Données mises à jour dans la table : {table_ref}")

# Point d'entrée pour Cloud Run
@app.route("/", methods=["POST"])
def run_pipeline():
    try:
        # Étape 1 : Déterminer l'heure de la dernière exécution (5 minute avant)
        last_execution_time = (datetime.datetime.utcnow() - datetime.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Étape 2 : Collecter les nouvelles données
        raw_data = collect_african_air_quality(last_execution_time)

        # Étape 3 : Transformer les données
        if raw_data:
            new_data = transform_air_quality_data(raw_data)
            print(f"{len(new_data)} nouvelles entrées collectées.")

            # Étape 4 : Écraser et charger les nouvelles données dans BigQuery
            PROJECT_ID = "lustrous-braid-415120"
            DATASET_ID = "qualite_air"
            TABLE_ID = "mesures_africaines"
            load_to_bigquery(new_data, PROJECT_ID, DATASET_ID, TABLE_ID)
        else:
            print("Aucune nouvelle donnée à ajouter.")
        return jsonify({"status": "success", "message": "Pipeline exécuté avec succès"})
    except Exception as e:
        print(f"Erreur : {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
