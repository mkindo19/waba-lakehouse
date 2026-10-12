# Image du Job de synchronisation OpenMetadata (niveau 4.3) : connecteurs officiels Trino + Kafka et SDK Python,
# même version que le serveur (1.10.14). cachetools : dépendance manquante du paquet 1.10.14.x (vécu en test local).
FROM python:3.11-slim
RUN pip install --no-cache-dir "openmetadata-ingestion[trino,kafka]==1.10.14.7" "cachetools==7.2.1"
WORKDIR /app
COPY sync.py catalog.yaml ./
RUN useradd --create-home --uid 1000 app
USER app
CMD ["python", "/app/sync.py"]
