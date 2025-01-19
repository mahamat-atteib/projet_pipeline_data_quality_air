# Utiliser une image Python officielle
FROM python:3.9-slim

# Copier le fichier des dépendances
COPY requirements.txt /app/requirements.txt

# Installer les dépendances
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copier le script dans l'image Docker
#COPY air_quality_pipeline.py /app/air_quality_pipeline.py
COPY . /app

# Définir le répertoire de travail
WORKDIR /app

# Exposer le port attendu par Cloud Run
EXPOSE 8080

# Commande par défaut pour exécuter le script
CMD ["python", "air_quality_pipeline.py"]
