# Utiliser une image Python officielle
FROM python:3.9-slim

# Installer les dépendances
RUN pip install pandas google-cloud-storage requests

# Copier le script dans l'image Docker
#COPY air_quality_pipeline.py /app/air_quality_pipeline.py
COPY . /app

# Définir le répertoire de travail
WORKDIR /app

# Exposer le port attendu par Cloud Run
EXPOSE 8080

# Commande par défaut pour exécuter le script
CMD ["python", "air_quality_pipeline.py"]
