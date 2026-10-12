#!/usr/bin/env python3
"""Niveau 4.3 — Démonstration du SSO Keycloak sur Trino : requêtes SQL avec un compte Keycloak.

    python scripts/trino_sso.py            (make k8s-trino-sso ; make k8s-expose doit tourner)

Le client affiche une URL : l'ouvrir dans le navigateur (fenêtre privée), se connecter avec un compte de
démonstration ; le script reprend seul. Les mêmes requêtes donnent un résultat différent selon le rôle :
pays visibles (analyste), tables autorisées (conformité, lecteur), catalogue Kafka (administrateur).
Changer de compte : fermer la fenêtre privée (sinon la session Keycloak reconnecte le même utilisateur).
"""

from __future__ import annotations

import argparse
import warnings

CHECKS = [
    ("Identité vue par Trino", "SELECT current_user, current_groups()"),
    ("Schémas visibles (catalogue iceberg)", "SHOW SCHEMAS FROM iceberg"),
    ("Silver : transactions par pays (filtre pays)",
     "SELECT country_code, count(*) FROM iceberg.silver.bank_transactions GROUP BY 1 ORDER BY 1"),
    ("Silver : IBAN pseudonymisés (donnée sensible)",
     "SELECT country_code, iban_masked FROM iceberg.silver.accounts LIMIT 2"),
    ("Gold réglementaire : NPL par pays", "SELECT DISTINCT country_code FROM iceberg.gold.npl_ratio_by_country ORDER BY 1"),
    ("Gold temps réel : événements AML individuels", "SELECT count(*) FROM iceberg.gold.rt_aml_events"),
    ("Catalogue Kafka (topics bruts)", "SHOW TABLES FROM kafka.default"),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default="trino.localtest.me")
    ap.add_argument("--port", type=int, default=8443)
    args = ap.parse_args()

    from trino.auth import CompositeRedirectHandler, ConsoleRedirectHandler, OAuth2Authentication

    import trino

    # Le client interroge Trino pour savoir si la connexion Keycloak est faite : 5 tentatives par défaut
    # (~1 min), trop court pour copier l'URL, accepter le certificat et se connecter -> ~10 min.
    trino.auth._OAuth2TokenBearer.MAX_OAUTH_ATTEMPTS = 60
    # Certificat auto-signé de l'Ingress (plateforme locale) : vérification TLS désactivée ICI seulement
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    conn = trino.dbapi.connect(
        host=args.host, port=args.port, http_scheme="https", verify=False,
        auth=OAuth2Authentication(CompositeRedirectHandler([ConsoleRedirectHandler()])),
    )
    print("Connexion Keycloak : ouvrir l'URL ci-dessous dans une fenêtre privée (10 min max).", flush=True)
    for title, sql in CHECKS:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            shown = ", ".join(" ".join(str(v) for v in r) for r in rows[:10])
            print(f"[OK]      {title} : {shown}{' ...' if len(rows) > 10 else ''}")
        except trino.exceptions.TrinoQueryError as e:
            reason = "accès refusé" if e.error_name == "PERMISSION_DENIED" else e.error_name
            print(f"[REFUSÉ]  {title} : {reason} ({e.message[:110]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
