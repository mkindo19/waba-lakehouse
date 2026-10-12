#!/usr/bin/env python3
"""Niveau 4.3 — Rôles Superset et filtres pays (RLS) « as code », appliqués après l'import des dashboards.

    python apply_security.py

Les rôles Keycloak arrivent dans Superset par AUTH_ROLES_MAPPING (superset_config.py) :

    Keycloak            Superset            accès
    group_admin         Admin               tout
    country_analyst     WABA_Analyste       3 dashboards + exploration, lignes de SON pays (RLS)
    compliance_officer  WABA_Conformite     D2 Risque & Conformité (tables gold réglementaires), tous pays
    viewer              WABA_Lecteur        D1 et D3 en lecture seule : ni exploration, ni « drill to detail »,
                                            ni export -> aucune ligne individuelle ; pas de D2 (événements AML)
    groupe pays_XX      WABA_Pays_XX        filtre RLS country_code = 'XX'

Aucun rôle WABA n'a SQL Lab : les filtres RLS ne s'appliquent pas au SQL libre (cf. D33).
Idempotent : à chaque démarrage, permissions, filtres et rôles des dashboards sont remis à l'état décrit ici.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_assets import CHARTS, DASHBOARDS, DATASETS  # noqa: E402

ALL_DASHBOARDS = list(DASHBOARDS)
ROLES: dict[str, dict] = {
    "WABA_Analyste": {"dashboards": ALL_DASHBOARDS, "explore": True},
    "WABA_Conformite": {"dashboards": ["d2_risque_conformite"], "explore": True},
    "WABA_Lecteur": {"dashboards": ["d1_performance_commerciale", "d3_mobile_money"], "explore": False},
}
# Permissions retirées au rôle Lecteur (en plus de celles d'écriture) : tout ce qui montre des lignes
# individuelles ou permet d'extraire les données.
NO_ROW_LEVEL = {
    ("can_explore", "Superset"), ("can_explore_json", "Superset"), ("can_drill", "Dashboard"),
    ("can_samples", "Datasource"), ("can_csv", "Superset"), ("can_export", "Chart"), ("can_export", "Dashboard"),
    ("can_export", "Dataset"), ("can_view_query", "Dashboard"), ("can_view_chart_as_table", "Dashboard"),
    ("can_share_chart", "Superset"), ("can_share_dashboard", "Superset"), ("can_read", "Explore"),
    ("can_read", "ExploreFormDataRestApi"), ("can_read", "ExplorePermalinkRestApi"),
}
WRITE_PERMS = {"can_write", "can_add", "can_edit", "can_delete", "can_save", "muldelete", "can_import_"}
RLS_GROUP = "pays"                      # filtres d'un même groupe combinés par OR


def datasets_of(dashboard: str) -> set[str]:
    return {CHARTS[chart]["ds"] for row in DASHBOARDS[dashboard]["rows"] for chart, _, _ in row}


def table_name(dataset: str) -> str:
    return DATASETS[dataset].get("table", dataset)


def main() -> int:
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset.connectors.sqla.models import RowLevelSecurityFilter, SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.utils.core import RowLevelSecurityFilterType

        from superset import db
        from superset import security_manager as sm

        countries = app.config["WABA_COUNTRIES"]
        tables = {name: db.session.query(SqlaTable).filter_by(table_name=table_name(name)).one() for name in DATASETS}
        dashboards = {name: db.session.query(Dashboard).filter_by(slug=d["slug"]).one() for name, d in DASHBOARDS.items()}
        gamma = sm.find_role("Gamma")

        def base_permissions(explore: bool) -> list:
            perms = []
            for pv in gamma.permissions:
                key = (pv.permission.name, pv.view_menu.name)
                if any(pv.permission.name.startswith(w) for w in WRITE_PERMS) and not explore:
                    continue
                if not explore and key in NO_ROW_LEVEL:
                    continue
                perms.append(pv)
            return perms

        role_objs = {}
        for role_name, spec in ROLES.items():
            role = sm.find_role(role_name) or sm.add_role(role_name)
            needed = set().union(*(datasets_of(d) for d in spec["dashboards"]))
            if role_name == "WABA_Analyste":
                needed = set(DATASETS)
            perms = base_permissions(spec["explore"])
            for ds in sorted(needed):
                pv = sm.add_permission_view_menu("datasource_access", tables[ds].perm)
                perms.append(pv)
            role.permissions = perms
            role_objs[role_name] = role
            print(f"{role_name:16s} {len(perms):3d} permissions, jeux de données : {', '.join(sorted(needed))}")

        # Rôles pays (sans permission propre : ils portent uniquement le filtre RLS)
        country_roles = {c: sm.find_role(f"WABA_Pays_{c}") or sm.add_role(f"WABA_Pays_{c}") for c in countries}

        # Filtres RLS : un par pays + un filtre « aucune ligne » pour l'analyste sans groupe pays (sécurité par
        # défaut). Même group_key -> combinés par OR : analyste CI = (1 = 0) OR (country_code = 'CI').
        all_tables = list(tables.values())
        wanted = {f"waba_pays_{c}": (f"country_code = '{c}'", [country_roles[c]]) for c in countries}
        wanted["waba_analyste_sans_pays"] = ("1 = 0", [role_objs["WABA_Analyste"]])
        existing = {f.name: f for f in db.session.query(RowLevelSecurityFilter).filter(
            RowLevelSecurityFilter.name.like("waba_%")).all()}
        for name, (clause, roles) in wanted.items():
            f = existing.pop(name, None) or RowLevelSecurityFilter(name=name)
            f.filter_type = RowLevelSecurityFilterType.REGULAR
            f.group_key = RLS_GROUP
            f.clause = clause
            f.roles = roles
            f.tables = all_tables
            f.description = "Restriction pays (rôle Keycloak country_analyst + groupe pays_XX), cf. D33"
            db.session.add(f)
        for obsolete in existing.values():
            db.session.delete(obsolete)

        # Dashboards : visibles uniquement des rôles autorisés (DASHBOARD_RBAC) ; Admin voit tout
        for name, dash in dashboards.items():
            dash.roles = [role_objs[r] for r, spec in ROLES.items() if name in spec["dashboards"]]
            print(f"dashboard {DASHBOARDS[name]['slug']:22s} -> {', '.join(r.name for r in dash.roles)}")
        db.session.commit()
        print(f"{len(wanted)} filtres RLS (groupe « {RLS_GROUP} ») sur {len(all_tables)} jeux de données")
    return 0


if __name__ == "__main__":
    sys.exit(main())
