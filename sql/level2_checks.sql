-- =============================================================================
-- Niveau 2 — Contrôles de l'architecture médaillon et des KPIs Gold (Trino)
-- Lancer : docker compose exec -T trino trino --catalog iceberg < sql/level2_checks.sql
-- =============================================================================

-- 1. Les 3 zones existent
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;

-- 2. Volumétrie par couche (même ordre de grandeur attendu)
SELECT 'raw' AS couche, count(*) FROM iceberg.raw.bank_transactions
UNION ALL SELECT 'bronze', count(*) FROM iceberg.bronze.bank_transactions
UNION ALL SELECT 'silver', count(*) FROM iceberg.silver.bank_transactions;

-- 3. Silver : dédoublonnage (doit renvoyer 0 ligne)
SELECT transaction_id, count(*) FROM iceberg.silver.bank_transactions GROUP BY 1 HAVING count(*) > 1;

-- 4. Silver : conversion EUR (XOF = parité fixe 655,957)
SELECT currency, round(avg(amount / nullif(amount_eur, 0)), 3) AS taux_observe
FROM iceberg.silver.bank_transactions GROUP BY 1;

-- 5. Gold : volume quotidien (exemple CI, 5 derniers jours)
SELECT txn_date, sum(txn_count) AS nb, round(sum(total_amount_eur), 0) AS montant_eur
FROM iceberg.gold.daily_transaction_volume WHERE country_code = 'CI'
GROUP BY 1 ORDER BY 1 DESC LIMIT 5;

-- 6. Gold : NPL par pays (seuil BCEAO 5 %)
SELECT country_code, period_month, npl_ratio, risk_level, is_above_threshold
FROM iceberg.gold.npl_ratio_by_country WHERE loan_type = 'ALL' ORDER BY country_code, period_month;

-- 7. Gold : loss ratio par pays (seuil CIMA 70 %)
SELECT country_code, period_month, loss_ratio, is_above_threshold
FROM iceberg.gold.loss_ratio_by_product WHERE product_line = 'ALL' ORDER BY country_code, period_month;

-- 8. Gold : ARPC mensuel
SELECT country_code, period_month, segment, active_customers, arpc_eur
FROM iceberg.gold.customer_arpu_monthly ORDER BY country_code, period_month, segment LIMIT 20;

-- 9. Gold : délai de traitement des sinistres vs SLA
SELECT country_code, line_group, round(avg(avg_processing_days), 1) AS delai_moyen, max(sla_days) AS sla
FROM iceberg.gold.claims_processing_time GROUP BY 1, 2 ORDER BY 1, 2;

-- 10. Gold : mobile money et corridors transfrontaliers
SELECT country_code, sum(txn_count) AS nb, round(avg(failure_rate), 4) AS taux_echec, sum(active_users) AS utilisateurs
FROM iceberg.gold.mobile_money_daily_flow GROUP BY 1 ORDER BY 1;
SELECT corridor, sum(transfers_count) AS nb, round(sum(total_amount_eur), 0) AS montant_eur
FROM iceberg.gold.cross_border_transfers GROUP BY 1 ORDER BY 3 DESC LIMIT 5;

-- 11. Filtrage par pays (critère : KPIs requêtables avec filtre pays)
SELECT * FROM iceberg.gold.npl_ratio_by_country WHERE country_code = 'SN' AND loan_type = 'ALL';

-- 12. Rapport réglementaire J+1
SELECT * FROM iceberg.gold.regulatory_daily_report ORDER BY report_date DESC, country_code LIMIT 16;
