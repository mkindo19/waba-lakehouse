# Plan de réalisation — 7 jours, 4 niveaux

Principe : **chaque soir, quelque chose de démontrable et commité**. On ne passe au niveau
suivant que si les critères d'évaluation du niveau courant sont cochés.

| Jour | Objectif | Livrable du soir | Critères cochés |
|---|---|---|---|
| **J1** | Socle : dépôt, Docker Compose, générateur + Streamlit, MinIO | `make up` → Streamlit dépose les CSV dans `raw-landing` | Streamlit (2 modes), cohérence référentielle, multi-pays |
| **J2** | Spark → Iceberg `raw.*` → Trino, idempotence, rejets, archivage | 8 tables `raw.*` requêtables dans Trino | **Niveau 1 complet** |
| **J3** | Airflow + couches Bronze / Silver (dédoublonnage, EUR, jointures) | `dag_ingest_raw` → `dag_bronze_to_silver` verts | DAGs, médaillon, Silver de qualité |
| **J4** | Gold : 7 KPIs + `dag_regulatory_report` (J+1 00h30) | 7 tables `gold.*` filtrables par pays | **Niveau 2 complet** |
| **J5** | NiFi → Kafka → Spark Streaming (Silver + fraude/AML), DLQ, Trino Kafka | Alerte fraude visible dans `gold-fraud-alerts` + requête Lambda | **Niveau 3 complet** |
| **J6** | Kubernetes (Helm), Superset (3 dashboards), Keycloak, OpenMetadata, Prometheus/Loki/Grafana | `helm install` sur Minikube + dashboards | Niveau 4 (cœur) |
| **J7** | Alertes Grafana testées, write-up (2-5 p.), README final, vidéo 5-10 min | Dépôt prêt + vidéo | **Livraison** |

## Risques et parades

- **Niveau 4 très chargé en une journée** → les manifestes Helm sont préparés au fil de l'eau
  (J3-J5), Keycloak/OpenMetadata utilisent leurs charts officiels, et le write-up
  documente honnêtement ce qui serait fait différemment en production.
- **Ressources (32 Go)** → profils Docker Compose : on ne lance que ce dont on a besoin
  (ex. NiFi + Kafka seulement au J5).
- **Windows / WSL2** → cloner le dépôt DANS le système de fichiers WSL (`~/`), pas sous
  `/mnt/c` (montages 5 à 10x plus lents) ; fins de ligne LF imposées par `.gitattributes`.

## Répartition du travail

- Claude écrit le code, la configuration et la documentation, et les teste autant que
  possible hors Docker (tests unitaires, Spark local).
- Mohamed lance la stack sur sa machine, remonte les erreurs (logs), valide les critères,
  pousse sur Git et enregistre la vidéo.
