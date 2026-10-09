#!/usr/bin/env python3
"""Génère helm/values-secrets.yaml à partir de .env (niveau 4, Kubernetes).

- Les secrets ne sont jamais écrits dans le chart ni dans le dépôt : ce fichier est ignoré par git
  (.gitignore) et créé avec les droits 600.
- Format JSON (le JSON est du YAML valide) : aucune dépendance, aucun problème d'échappement.
- Seules les variables sensibles sont reprises (mots de passe, clés, identifiants, sels).
En production : External Secrets / Vault / Sealed Secrets à la place de ce fichier (cf. D26).
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV, OUT = ROOT / ".env", ROOT / "helm" / "values-secrets.yaml"
SENSITIVE = re.compile(r"(PASSWORD|SECRET|_KEY$|_USER$|SALT|TOKEN)")
EXCLUDE = re.compile(r"(_IMAGE|_PORT|_VERSION)$")


def parse(path: Path) -> dict[str, str]:
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def main() -> int:
    if not ENV.exists():
        print("Fichier .env absent : lancer d'abord `make env`.", file=sys.stderr)
        return 1
    env = parse(ENV)
    secrets = {k: v for k, v in sorted(env.items()) if SENSITIVE.search(k) and not EXCLUDE.search(k)}
    weak = [k for k, v in secrets.items() if v.startswith("change-me") or not v]
    if weak:
        print(f"Secrets non générés dans .env : {weak} (lancer `make env`).", file=sys.stderr)
        return 1
    OUT.write_text(json.dumps({"secrets": secrets}, indent=2) + "\n")
    os.chmod(OUT, 0o600)
    print(f"{OUT.relative_to(ROOT)} : {len(secrets)} secrets ({', '.join(secrets)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
