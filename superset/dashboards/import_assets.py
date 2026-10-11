#!/usr/bin/env python3
"""Importe (en écrasant) le paquet de dashboards produit par build_assets.py.

    python import_assets.py <zip> <utilisateur propriétaire>

`superset import-dashboards` n'écrase que les dashboards : un graphique ou un jeu de données déjà importé
ne serait jamais mis à jour. ImportAssetsCommand (celle de l'API /api/v1/assets/import) écrase tout :
le dépôt reste la seule source de vérité des dashboards.
"""

from __future__ import annotations

import sys
from zipfile import ZipFile


def main(path: str, username: str) -> int:
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from flask import g
        from superset.commands.importers.v1.assets import ImportAssetsCommand
        from superset.commands.importers.v1.utils import get_contents_from_bundle

        from superset import security_manager

        g.user = security_manager.find_user(username=username)
        with ZipFile(path) as bundle:
            contents = get_contents_from_bundle(bundle)
        ImportAssetsCommand(contents).run()
        print(f"{path} importé ({len(contents)} fichiers)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
