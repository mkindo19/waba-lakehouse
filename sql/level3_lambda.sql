-- =============================================================================
-- Niveau 3.4 — Requête Lambda unifiée : batch (Iceberg gold) + temps réel (Kafka) dans une même requête
-- Lancer : make lambda   (= docker compose exec -T trino trino --catalog iceberg < sql/level3_lambda.sql)
-- =============================================================================

-- 1. Requête de l'énoncé, telle quelle : elle s'exécute sans erreur (critère d'évaluation).
--    Attention : elle joint des LIGNES (une par transaction côté Kafka, une par entité x type côté gold)
--    et additionne batch + streaming sur les mêmes jours -> montants multipliés / comptés deux fois.
SELECT
    COALESCE(b.country_code, s.country_code) AS country,
    COALESCE(b.txn_date, CAST(s.event_time AS DATE)) AS date,
    COALESCE(b.total_amount_eur, 0) + COALESCE(s.streaming_amount_eur, 0) AS total_eur
FROM gold.daily_transaction_volume b
FULL OUTER JOIN kafka.default."silver-bank-transactions" s
    ON b.country_code = s.country_code
    AND b.txn_date = CAST(s.event_time AS DATE)
ORDER BY 1, 2
LIMIT 20;

-- 2. Version corrigée (principe Lambda) : chaque côté est d'abord agrégé par pays et jour, et la couche
--    temps réel ne couvre que les jours que le batch n'a PAS encore traités pour ce pays (après la
--    dernière date gold). Quand le batch passe, il remplace automatiquement le temps réel.
--    Les transactions de test (make alert-test, 100 milliards XOF) sont exclues.
WITH batch AS (
    SELECT country_code, txn_date, sum(txn_count) AS txn_count, sum(total_amount_eur) AS amount_eur
    FROM gold.daily_transaction_volume
    GROUP BY 1, 2
),
batch_end AS (
    SELECT country_code, max(txn_date) AS last_batch_date FROM batch GROUP BY 1
),
speed AS (
    SELECT s.country_code, CAST(s.event_time AS DATE) AS txn_date,
           count(*) AS txn_count,
           sum(s.streaming_amount_eur) FILTER (WHERE s.transaction_status = 'SUCCESS') AS amount_eur
    FROM kafka.default."silver-bank-transactions" s
    LEFT JOIN batch_end e ON e.country_code = s.country_code
    WHERE s.transaction_id NOT LIKE 'ALERT-TEST-%'
      AND (e.last_batch_date IS NULL OR CAST(s.event_time AS DATE) > e.last_batch_date)
    GROUP BY 1, 2
)
SELECT COALESCE(b.country_code, s.country_code) AS country,
       COALESCE(b.txn_date, s.txn_date) AS date,
       COALESCE(b.txn_count, 0) + COALESCE(s.txn_count, 0) AS txn_count,
       round(COALESCE(b.amount_eur, 0) + COALESCE(s.amount_eur, 0), 2) AS total_eur,
       CASE WHEN b.txn_date IS NOT NULL THEN 'batch (Iceberg)' ELSE 'temps réel (Kafka)' END AS source
FROM batch b
FULL OUTER JOIN speed s ON b.country_code = s.country_code AND b.txn_date = s.txn_date
WHERE COALESCE(b.txn_date, s.txn_date) >= (SELECT min(last_batch_date) FROM batch_end) - INTERVAL '3' DAY  -- fin du batch + temps réel
ORDER BY 1, 2;
