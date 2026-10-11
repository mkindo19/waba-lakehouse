"""Configuration Superset WABA (niveau 4). Montée dans /app/pythonpath par le chart Helm.

Aucun secret ici : clé de session et mot de passe de la base viennent du Secret Kubernetes « waba-secrets »
(variables d'environnement).
"""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{os.environ['SUPERSET_DB_PASSWORD']}"
    f"@{os.environ.get('SUPERSET_DB_HOST', 'superset-db')}:5432/superset"
)

# Derrière l'Ingress NGINX (en-têtes X-Forwarded-*)
ENABLE_PROXY_FIX = True
# En-têtes de sécurité HTTPS (Talisman) désactivés : accès local en HTTP. En production : HTTPS + CSP.
TALISMAN_ENABLED = False

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,          # accès aux dashboards par rôle (4.3 : rôles Keycloak)
    "DRILL_TO_DETAIL": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# Cache en mémoire du processus (un seul pod) ; en production : Redis
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}

BABEL_DEFAULT_LOCALE = "fr"
LANGUAGES = {"fr": {"flag": "fr", "name": "Français"}, "en": {"flag": "us", "name": "English"}}

ROW_LIMIT = 50000
SUPERSET_WEBSERVER_TIMEOUT = 120
