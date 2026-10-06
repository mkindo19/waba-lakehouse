#!/bin/bash
# Création idempotente des topics Kafka du niveau 3 (--if-not-exists : relançable sans erreur).
#   raw-*    : événements bruts publiés par NiFi (1 message = 1 ligne CSV en JSON)
#   silver-* : événements validés / enrichis par le job Spark Streaming 1
#   gold-*   : alertes produites par le job Spark Streaming 2
#   dlq-*    : messages malformés (Dead Letter Queue), conservés plus longtemps pour analyse
set -euo pipefail

BOOTSTRAP="kafka:9092"
KT=/opt/kafka/bin/kafka-topics.sh
PARTITIONS=3                       # 3 partitions : parallélisme, clé = country_code
WEEK=$((7 * 24 * 3600 * 1000))     # rétention 7 jours (ms)
MONTH=$((30 * 24 * 3600 * 1000))   # rétention 30 jours pour les alertes et la DLQ

create() {   # create <topic> <rétention ms>
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$1" --partitions "$PARTITIONS" --replication-factor 1 \
        --config retention.ms="$2" --config cleanup.policy=delete
}

for t in raw-bank-transactions raw-insurance-operations raw-mobile-money-payments raw-loan-repayments \
         silver-bank-transactions silver-insurance-operations silver-mobile-money; do
  create "$t" "$WEEK"
done
for t in gold-fraud-alerts gold-aml-events gold-liquidity-alerts dlq-financial-events; do
  create "$t" "$MONTH"
done

echo "Topics présents :"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
