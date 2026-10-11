# Image Superset WABA (niveau 4) : image officielle + pilotes PostgreSQL (base de métadonnées) et Trino (SQLAlchemy)
# + Authlib (SSO Keycloak, 4.3). Depuis Superset 4.1, l'image officielle « lean » ne contient plus AUCUN pilote de
# base, pas même psycopg2 (vécu : ModuleNotFoundError au démarrage, cf. D29).
# La configuration (superset_config.py) et les dashboards « as code » ne sont PAS dans l'image : ils sont
# montés depuis des ConfigMaps du chart Helm (modifiables sans reconstruire l'image).
ARG SUPERSET_IMAGE=apache/superset:4.1.1
FROM ${SUPERSET_IMAGE}
USER root
RUN pip install --no-cache-dir "psycopg2-binary==2.9.9" "trino[sqlalchemy]==0.330.0" "authlib==1.3.2"
USER superset
