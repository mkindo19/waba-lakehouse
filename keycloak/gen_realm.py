#!/usr/bin/env python3
"""Génère keycloak/waba-realm.json (niveau 4.3 — SSO Keycloak) : `python keycloak/gen_realm.py`.

Le realm est décrit ici en Python (listes de pays, utilisateurs de démonstration) puis écrit en JSON, format
d'import natif de Keycloak. Les secrets ne sont PAS dans le fichier : placeholders ${VAR} remplacés par
Keycloak à l'import, depuis les variables d'environnement du pod (Secret Kubernetes « waba-secrets »).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
UI_PORT = 8090                       # port HTTP de make k8s-expose (cf. values.yaml ingress.port)
TLS_PORT = 8443                      # port HTTPS de make k8s-expose : Trino exige HTTPS pour OAuth2 (cf. D34)
DOMAIN = "localtest.me"


def url(host: str, tls: bool = False) -> str:
    return f"https://{host}.{DOMAIN}:{TLS_PORT}" if tls else f"http://{host}.{DOMAIN}:{UI_PORT}"


# Les 4 rôles de l'énoncé (rôles de realm) ; un utilisateur les reçoit via son groupe fonctionnel.
ROLES = {
    "group_admin": "Administrateur groupe : toutes les entités, tous les pays, toutes les données",
    "country_analyst": "Analyste pays : données de SON pays uniquement (groupe pays_XX)",
    "compliance_officer": "Conformité : tables gold réglementaires uniquement (BCEAO, CIMA), tous pays",
    "viewer": "Lecteur : dashboards agrégés uniquement, aucune donnée sensible (IBAN, comptes, montants individuels)",
}
FUNCTIONAL_GROUPS = {"administrateurs": "group_admin", "analystes_pays": "country_analyst",
                     "conformite": "compliance_officer", "lecteurs": "viewer"}

# Utilisateurs de démonstration : un par rôle (+ un 2e analyste pour montrer la restriction pays).
# Mot de passe commun ${KEYCLOAK_DEMO_PASSWORD} (.env), à changer à la 1re connexion en production.
USERS = [
    ("admin.groupe", "Awa", "Diop", ["administrateurs"]),
    ("analyste.ci", "Koffi", "Kouassi", ["analystes_pays", "pays_CI"]),
    ("analyste.sn", "Fatou", "Ndiaye", ["analystes_pays", "pays_SN"]),
    ("conformite", "Ibrahim", "Traoré", ["conformite"]),
    ("lecteur", "Ama", "Mensah", ["lecteurs"]),
]


def mapper(name: str, kind: str, claim: str, **extra: str) -> dict:
    """Mappeur de jeton OIDC : ajoute `claim` au jeton d'accès, au jeton d'identité et à userinfo."""
    return {"name": name, "protocol": "openid-connect", "protocolMapper": kind,
            "config": {"claim.name": claim, "jsonType.label": "String", "multivalued": "true",
                       "access.token.claim": "true", "id.token.claim": "true", "userinfo.token.claim": "true",
                       "introspection.token.claim": "true", **extra}}


# Toutes les applications reçoivent les mêmes informations d'autorisation :
#   roles  = rôles de realm effectifs (hérités du groupe fonctionnel) -> profil fonctionnel
#   groups = groupes (sans chemin) -> pays autorisés (pays_XX) + groupe fonctionnel
WABA_MAPPERS = [
    mapper("roles WABA", "oidc-usermodel-realm-role-mapper", "roles", **{"usermodel.realmRoleMapping.rolePrefix": ""}),
    mapper("groupes WABA", "oidc-group-membership-mapper", "groups", **{"full.path": "false"}),
]


def client(client_id: str, name: str, secret_var: str, redirects: list[str], base: str, extra_mappers=(),
           pkce: bool = True) -> dict:
    attributes = {"post.logout.redirect.uris": f"{base}/*"}
    if pkce:                     # PKCE exigé (protection du code d'autorisation) si l'application le gère
        attributes["pkce.code.challenge.method"] = "S256"
    return {
        "clientId": client_id, "name": name, "enabled": True, "protocol": "openid-connect",
        "publicClient": False, "clientAuthenticatorType": "client-secret", "secret": f"${{{secret_var}}}",
        "standardFlowEnabled": True, "directAccessGrantsEnabled": False, "serviceAccountsEnabled": False,
        "implicitFlowEnabled": False, "frontchannelLogout": True,
        "redirectUris": redirects, "webOrigins": ["+"], "rootUrl": base, "baseUrl": "/",
        "attributes": attributes,
        "defaultClientScopes": ["basic", "profile", "email", "web-origins", "acr"],
        "optionalClientScopes": ["offline_access"],
        "protocolMappers": WABA_MAPPERS + list(extra_mappers),
    }


def realm() -> dict:
    superset, trino = url("superset"), url("trino", tls=True)
    return {
        "realm": "waba", "displayName": "WABA Group", "enabled": True,
        # HTTP accepté : plateforme locale (minikube). Production : "external" (HTTPS obligatoire hors réseau privé)
        "sslRequired": "none",
        "internationalizationEnabled": True, "supportedLocales": ["fr", "en"], "defaultLocale": "fr",
        "loginWithEmailAllowed": True, "registrationAllowed": False, "resetPasswordAllowed": False,
        "rememberMe": False, "bruteForceProtected": True, "failureFactor": 5,
        "accessTokenLifespan": 300, "ssoSessionIdleTimeout": 1800, "ssoSessionMaxLifespan": 36000,
        "passwordPolicy": "length(12) and upperCase(1) and lowerCase(1) and digits(1) and notUsername",
        "roles": {"realm": [{"name": r, "description": d} for r, d in ROLES.items()]},
        "groups": (
            [{"name": g, "path": f"/{g}", "realmRoles": [r]} for g, r in FUNCTIONAL_GROUPS.items()]
            + [{"name": f"pays_{c}", "path": f"/pays_{c}", "attributes": {"country_code": [c]}} for c in COUNTRIES]
        ),
        "users": [
            {"username": u, "enabled": True, "emailVerified": True, "firstName": fn, "lastName": ln,
             "email": f"{u}@waba-group.example", "groups": [f"/{g}" for g in groups],
             "credentials": [{"type": "password", "value": "${KEYCLOAK_DEMO_PASSWORD}", "temporary": False}]}
            for u, fn, ln, groups in USERS
        ],
        "clients": [
            client("superset", "Superset (dashboards)", "SUPERSET_OIDC_CLIENT_SECRET",
                   [f"{superset}/oauth-authorized/keycloak"], superset),
            # Trino vérifie que son client-id figure dans l'audience (aud) du jeton d'accès
            client("trino", "Trino (SQL)", "TRINO_OIDC_CLIENT_SECRET",
                   [f"{trino}/oauth2/callback", f"{trino}/ui/logout/logout.html"], trino,
                   [{"name": "audience trino", "protocol": "openid-connect", "protocolMapper": "oidc-audience-mapper",
                     "config": {"included.client.audience": "trino", "access.token.claim": "true",
                                "id.token.claim": "false", "introspection.token.claim": "true"}}],
                   pkce=False),          # Trino 458 n'envoie pas de code_challenge PKCE
        ],
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ui-port", type=int, default=UI_PORT, help="port des interfaces (tests locaux)")
    ap.add_argument("--out", type=Path, default=Path(__file__).with_name("waba-realm.json"))
    args = ap.parse_args()
    UI_PORT, out = args.ui_port, args.out
    out.write_text(json.dumps(realm(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{out} : {len(USERS)} utilisateurs, {len(ROLES)} rôles, {len(COUNTRIES)} groupes pays")
