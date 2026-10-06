#!/usr/bin/env bash
# Niveau 3 — publie dans raw-bank-transactions des transactions de test, horodatées MAINTENANT,
# qui doivent déclencher les alertes du job streaming 2 (après passage par le job 1) :
#   - 3 virements de 1 500 000 XOF du même compte en 2 min -> R1 rafale (gold-fraud-alerts)
#                                                            + 3 événements AML (gold-aml-events)
#   - 2 retraits de 100 milliards XOF d'un autre compte     -> alerte de liquidité (gold-liquidity-alerts)
#                                                            + R1 rafale sur ce 2e compte
# Usage : scripts/alert_test.sh [PAYS UEMOA, défaut SN]
set -euo pipefail

COUNTRY="${1:-SN}"
case "$COUNTRY" in CI|SN|ML|BF|GN|TG|BJ) ;; *) echo "pays UEMOA attendu (XOF), reçu : $COUNTRY" >&2; exit 1 ;; esac
RUN="$(date -u +%s)"
ts() { date -u -d "$1" +%Y-%m-%dT%H:%M:%S; }

msg() {  # id compte type montant horodatage
  printf '%s|{"transaction_id":"%s","timestamp":"%s","account_id":"%s","beneficiary_account":"","branch_id":"TEST","country_code":"%s","transaction_type":"%s","amount":"%s","currency":"XOF","channel":"MOBILE","transaction_status":"SUCCESS","fee_amount":"0","entity_type":"BANK","source_file":"alert-test","ingestion_timestamp":"%s.000Z"}\n' \
    "$COUNTRY" "$1" "$5" "$2" "$COUNTRY" "$3" "$4" "$(ts now)"
}

{
  msg "ALERT-TEST-$RUN-1" "TEST-$COUNTRY-BURST" TRANSFER 1500000 "$(ts '-2 min')"
  msg "ALERT-TEST-$RUN-2" "TEST-$COUNTRY-BURST" TRANSFER 1500000 "$(ts '-1 min')"
  msg "ALERT-TEST-$RUN-3" "TEST-$COUNTRY-BURST" TRANSFER 1500000 "$(ts now)"
  msg "ALERT-TEST-$RUN-4" "TEST-$COUNTRY-RUN" WITHDRAWAL 100000000000 "$(ts '-1 min')"
  msg "ALERT-TEST-$RUN-5" "TEST-$COUNTRY-RUN" WITHDRAWAL 100000000000 "$(ts now)"
} | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
      --topic raw-bank-transactions --property parse.key=true --property key.separator='|'

echo "5 transactions de test publiées ($COUNTRY, lot $RUN). Dans ~1 min : make gold-peek"
