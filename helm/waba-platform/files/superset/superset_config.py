"""Configuration Superset WABA (niveau 4). Montée dans /app/pythonpath par le chart Helm.

Aucun secret ici : clé de session, mot de passe de la base et secret du client OIDC viennent du Secret
Kubernetes « waba-secrets » (variables d'environnement).

Authentification (4.3) : SSO Keycloak (OIDC) dès que KEYCLOAK_PUBLIC_URL est défini, sinon comptes locaux.
Autorisation : rôles Keycloak -> rôles Superset (AUTH_ROLES_MAPPING, resynchronisés à chaque connexion) ;
permissions des rôles WABA_* et filtres pays (RLS) créés « as code » par dashboards/apply_security.py.
"""

import logging
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

# ============================================================================ SSO Keycloak (énoncé 4.3)
# Pays du groupe (mêmes codes que le générateur et que les groupes Keycloak pays_XX)
WABA_COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]

KEYCLOAK_PUBLIC_URL = os.environ.get("KEYCLOAK_PUBLIC_URL", "")       # vue par le navigateur
if KEYCLOAK_PUBLIC_URL:
    from flask import redirect, session
    from flask_appbuilder import expose
    from flask_appbuilder.security.manager import AUTH_OAUTH
    from flask_appbuilder.security.views import AuthOAuthView
    from flask_login import logout_user
    from superset.security import SupersetSecurityManager

    _log = logging.getLogger("waba.sso")
    _REALM = os.environ.get("KEYCLOAK_REALM", "waba")
    # Appels serveur à serveur (jetons, userinfo, clés) : adresse interne du cluster. Keycloak
    # (--hostname-backchannel-dynamic) annonce alors l'URL publique pour le navigateur et l'URL interne pour
    # le reste ; l'émetteur (iss) des jetons reste l'URL publique dans les deux cas.
    _KC_INTERNAL = os.environ.get("KEYCLOAK_INTERNAL_URL", KEYCLOAK_PUBLIC_URL)
    _SUPERSET_PUBLIC_URL = os.environ.get("SUPERSET_PUBLIC_URL", "http://superset.localtest.me:8090")

    AUTH_TYPE = AUTH_OAUTH
    OAUTH_PROVIDERS = [{
        "name": "keycloak", "icon": "fa-key", "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ.get("SUPERSET_OIDC_CLIENT_ID", "superset"),
            "client_secret": os.environ["SUPERSET_OIDC_CLIENT_SECRET"],
            "server_metadata_url": f"{_KC_INTERNAL}/realms/{_REALM}/.well-known/openid-configuration",
            "api_base_url": f"{_KC_INTERNAL}/realms/{_REALM}/protocol/",
            "client_kwargs": {"scope": "openid profile email", "code_challenge_method": "S256"},
        },
    }]

    # Rôle Keycloak (claim « roles ») ou groupe pays (claim « groups ») -> rôles Superset
    AUTH_ROLES_MAPPING = {
        "group_admin": ["Admin"],
        "country_analyst": ["WABA_Analyste"],
        "compliance_officer": ["WABA_Conformite"],
        "viewer": ["WABA_Lecteur"],
        **{f"pays_{c}": [f"WABA_Pays_{c}"] for c in WABA_COUNTRIES},
    }
    AUTH_ROLES_SYNC_AT_LOGIN = True          # Keycloak reste la source de vérité (retrait d'un rôle = effectif)
    AUTH_USER_REGISTRATION = True            # compte Superset créé à la 1re connexion SSO
    AUTH_USER_REGISTRATION_ROLE = "Public"   # sans rôle WABA : aucun accès (Public n'a aucune permission)

    class WabaAuthOAuthView(AuthOAuthView):
        """Déconnexion complète : session Superset ET session SSO Keycloak (sinon reconnexion automatique
        avec le même compte). Keycloak demande une confirmation, puis revient sur la page de connexion."""

        @expose("/logout/")
        def logout(self):
            logout_user()
            session.clear()
            return redirect(f"{KEYCLOAK_PUBLIC_URL}/realms/{_REALM}/protocol/openid-connect/logout"
                            f"?client_id=superset&post_logout_redirect_uri={_SUPERSET_PUBLIC_URL}/login/")

    class WabaSecurityManager(SupersetSecurityManager):
        authoauthview = WabaAuthOAuthView

        def oauth_user_info(self, provider, response=None):
            if provider != "keycloak":
                return super().oauth_user_info(provider, response)
            me = self.appbuilder.sm.oauth_remotes[provider].get("openid-connect/userinfo")
            me.raise_for_status()
            data = me.json()
            roles, groups = data.get("roles", []), data.get("groups", [])
            _log.info("connexion SSO %s : rôles %s, groupes %s", data.get("preferred_username"), roles, groups)
            return {
                "username": data.get("preferred_username", ""),
                "first_name": data.get("given_name", ""),
                "last_name": data.get("family_name", ""),
                "email": data.get("email", ""),
                "role_keys": roles + groups,
            }

    CUSTOM_SECURITY_MANAGER = WabaSecurityManager

    # Derrière l'Ingress : schéma et hôte (avec le port 8090) viennent de X-Forwarded-Proto / -Host. Le port
    # X-Forwarded-Port (80 = port interne du contrôleur NGINX) est ignoré, sinon l'URL de retour OIDC
    # deviendrait http://superset.localtest.me:80/... et serait refusée par Keycloak.
    PROXY_FIX_CONFIG = {"x_for": 1, "x_proto": 1, "x_host": 1, "x_port": 0, "x_prefix": 1}
