-- =============================================================================
-- Niveau 1.4 — Requêtes analytiques de base (Trino, catalogue iceberg)
-- Lancer : make trino   puis coller les requêtes
-- =============================================================================

-- 0. Les 8 tables raw.* existent
SHOW TABLES FROM iceberg.raw;

-- 1. Soldes par pays et par type de compte (devise locale)
SELECT country_code, currency, account_type,
       count(*)                  AS nb_comptes,
       round(sum(balance), 0)    AS solde_total
FROM iceberg.raw.accounts
WHERE status = 'ACTIVE'
GROUP BY 1, 2, 3
ORDER BY 1, 3;

-- 2. Volume et montant des transactions bancaires par pays et par jour
SELECT country_code, date("timestamp") AS jour,
       count(*)                AS nb_transactions,
       round(sum(amount), 0)   AS montant_total
FROM iceberg.raw.bank_transactions
WHERE transaction_status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY 1, 2;

-- 3. Comptages par entité (clients, comptes) et par pays
SELECT c.entity_type, c.country_code,
       count(DISTINCT c.customer_id) AS nb_clients,
       count(a.account_id)           AS nb_comptes
FROM iceberg.raw.customers c
LEFT JOIN iceberg.raw.accounts a ON a.customer_id = c.customer_id
GROUP BY 1, 2
ORDER BY 1, 2;

-- 4. Contrôle d'intégrité : aucune transaction orpheline (doit renvoyer 0)
SELECT count(*) AS orphelins
FROM iceberg.raw.bank_transactions t
LEFT JOIN iceberg.raw.accounts a ON a.account_id = t.account_id
WHERE a.account_id IS NULL;

-- 5. Contrôle d'idempotence : aucune clé en double (doit renvoyer 0 ligne)
SELECT transaction_id, count(*) FROM iceberg.raw.bank_transactions
GROUP BY 1 HAVING count(*) > 1;

-- 6. Rejets à l'ingestion par motif
SELECT dataset, reject_reason, count(*) AS nb
FROM iceberg.raw.rejected_records
GROUP BY 1, 2 ORDER BY 3 DESC;

-- 7. Données personnelles : l'IBAN n'est jamais stocké en clair
SELECT account_id, iban_masked, substr(iban_hash, 1, 16) AS iban_hash_prefix
FROM iceberg.raw.accounts LIMIT 5;
