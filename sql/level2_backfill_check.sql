-- Contrôle du backfill sélectif d'un pays (ex. dag_bronze_to_silver avec {"country_code": "SN"})
-- À lancer AVANT puis APRÈS le backfill (une fois les DAGs entièrement verts) :
--   docker compose exec -T trino trino --catalog iceberg < sql/level2_backfill_check.sql

-- 1. Volumes silver par pays : doivent être IDENTIQUES avant / après (idempotence)
SELECT country_code, count(*) AS lignes, round(sum(amount_eur), 2) AS montant_eur
FROM silver.bank_transactions
GROUP BY country_code
ORDER BY country_code;

-- 2. Derniers commits Iceberg de la table (element_at : NULL si la clé est absente du résumé)
SELECT committed_at, operation,
       element_at(summary, 'changed-partition-count') AS partitions_modifiees,
       element_at(summary, 'added-records')           AS lignes_ajoutees,
       element_at(summary, 'deleted-records')         AS lignes_supprimees
FROM silver."bank_transactions$snapshots"
ORDER BY committed_at DESC
LIMIT 3;

-- 3. Pays touchés par le dernier commit : bornes de la partition country_code des fichiers ajoutés.
--    Après un backfill SN : pays_min = pays_max = 'SN'
SELECT added_data_files_count AS fichiers_ajoutes,
       added_rows_count       AS lignes_ajoutees,
       partitions[1].lower_bound AS pays_min,
       partitions[1].upper_bound AS pays_max
FROM silver."bank_transactions$manifests"
WHERE added_data_files_count > 0
  AND added_snapshot_id = (SELECT snapshot_id FROM silver."bank_transactions$snapshots"
                           ORDER BY committed_at DESC LIMIT 1);
