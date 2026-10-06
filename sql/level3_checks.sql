-- =============================================================================
-- Niveau 3 — Contrôles du streaming (Trino : catalogues kafka + iceberg)
-- Lancer : make check3   (= docker compose exec -T trino trino --catalog iceberg < sql/level3_checks.sql)
-- =============================================================================

-- 1. Latence NiFi -> Kafka (critère < 30 s) : heure d'écriture dans Kafka (_timestamp) - heure
--    d'horodatage par NiFi (ingestion_timestamp). S'y ajoute au plus 10 s de détection du fichier (ListS3).
SELECT count(*) AS messages,
       round(approx_percentile(date_diff('millisecond', ingestion_timestamp, _timestamp), 0.5) / 1000.0, 2) AS p50_s,
       round(approx_percentile(date_diff('millisecond', ingestion_timestamp, _timestamp), 0.95) / 1000.0, 2) AS p95_s,
       round(max(date_diff('millisecond', ingestion_timestamp, _timestamp)) / 1000.0, 2) AS max_s
FROM kafka.default."raw-bank-transactions"
WHERE source_file LIKE 's3://%';

-- 2. Latence de bout en bout NiFi -> silver (job 1 compris)
SELECT round(approx_percentile(date_diff('millisecond', ingestion_timestamp, _timestamp), 0.5) / 1000.0, 2) AS p50_s,
       round(approx_percentile(date_diff('millisecond', ingestion_timestamp, _timestamp), 0.95) / 1000.0, 2) AS p95_s
FROM kafka.default."silver-bank-transactions"
WHERE source_file LIKE 's3://%';

-- 3. Double sink du job 1 : même nombre de transactions dans Kafka (silver-*) et dans Iceberg (silver.rt_*)
SELECT 'kafka silver-bank-transactions' AS sink, count(DISTINCT transaction_id) AS transactions
FROM kafka.default."silver-bank-transactions"
UNION ALL
SELECT 'iceberg silver.rt_bank_transactions', count(DISTINCT transaction_id) FROM silver.rt_bank_transactions;

-- 4. Dead Letter Queue : messages rejetés et motif (rien n'est ignoré silencieusement)
SELECT source_topic, reject_reason, count(*) AS messages
FROM kafka.default."dlq-financial-events" GROUP BY 1, 2 ORDER BY 3 DESC;

-- 5. Les 3 règles de fraude alertent (topic gold-fraud-alerts)
SELECT rule, severity, alert_status, count(*) AS messages
FROM kafka.default."gold-fraud-alerts" GROUP BY 1, 2, 3 ORDER BY 1;

-- 6. Vérité terrain R2 : alertes émises = paiements réellement émis hors du pays du client
SELECT 'attendu' AS k, count(*) AS n FROM silver.rt_mobile_money_payments m
JOIN silver.customers c ON c.customer_id = m.sender_id
WHERE m.sender_country <> c.country_code
UNION ALL
SELECT 'alertes', count(*) FROM gold.rt_fraud_alerts WHERE rule = 'UNUSUAL_COUNTRY';

-- 7. AML : événements par pays et régulateur (seuils 1 000 000 XOF / 5 000 GHS)
SELECT country_code, regulator, count(*) AS events, min(amount) AS min_local, max(threshold_local) AS seuil_local
FROM kafka.default."gold-aml-events" GROUP BY 1, 2 ORDER BY 1;

-- 8. Liquidité : alertes par pays
SELECT country_code, severity, coverage_ratio, round(net_outflow_eur, 0) AS net_outflow_eur,
       round(reference_liquidity_eur, 0) AS reference_eur, period_start, period_end
FROM kafka.default."gold-liquidity-alerts" ORDER BY detected_at DESC;
