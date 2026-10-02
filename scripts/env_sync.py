#!/usr/bin/env python3
"""Crée ou complète le fichier .env à partir de .env.example.

- Si .env n'existe pas : il est créé.
- S'il existe : seules les variables MANQUANTES sont ajoutées (les valeurs existantes
  ne sont jamais modifiées) — utile quand un nouveau niveau ajoute des variables.
- Toute valeur d'exemple commençant par "change-me" est remplacée par un secret aléatoire.
"""

from __future__ import annotations

import base64
import os
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE, TARGET = ROOT / ".env.example", ROOT / ".env"


def secret_for(key: str) -> str:
    if key == "AIRFLOW_FERNET_KEY":                       # format exigé par Airflow
        return base64.urlsafe_b64encode(os.urandom(32)).decode()
    return secrets.token_hex(24 if "SALT" in key else 16)


def parse(path: Path) -> dict[str, str]:
    out = {}
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def main() -> None:
    example, current = parse(EXAMPLE), parse(TARGET)
    missing = {k: v for k, v in example.items() if k not in current}
    if not missing:
        print(".env à jour : rien à ajouter")
        return
    lines = [f"{k}={secret_for(k) if v.startswith('change-me') else v}" for k, v in missing.items()]
    header = "" if not TARGET.exists() else "\n# --- Ajouté automatiquement par scripts/env_sync.py\n"
    with TARGET.open("a") as f:
        f.write(header + "\n".join(lines) + "\n")
    print(f".env : {len(missing)} variable(s) ajoutée(s) -> {', '.join(missing)}")


if __name__ == "__main__":
    main()
