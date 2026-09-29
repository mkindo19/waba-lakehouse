"""Utilitaires transverses : logging structuré JSON, IBAN, hachage déterministe."""

from __future__ import annotations

import json
import logging
import sys
import zlib
from datetime import datetime, timezone

import numpy as np


class JsonFormatter(logging.Formatter):
    """Formate chaque log en une ligne JSON (exploitable par Loki au niveau 4)."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Champs métier passés via logger.info("...", extra={"ctx": {...}})
        ctx = getattr(record, "ctx", None)
        if isinstance(ctx, dict):
            payload.update(ctx)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Retourne un logger JSON sur stdout (idempotent : pas de handlers dupliqués)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger


def stable_uniform(keys: np.ndarray | list[str]) -> np.ndarray:
    """Valeur pseudo-aléatoire stable dans [0, 1) dérivée d'une clé (CRC32).

    Permet de donner à un compte un "profil de risque" identique d'une
    exécution à l'autre (ex. compte prêt en défaut), sans stocker d'état.
    """
    return np.fromiter(
        (zlib.crc32(str(k).encode()) / 2**32 for k in keys), dtype=float, count=len(keys)
    )


def _iban_check_digits(country: str, bban: str) -> str:
    """Clé de contrôle IBAN (ISO 13616, modulo 97)."""
    rearranged = bban + country + "00"
    numeric = "".join(str(int(ch, 36)) for ch in rearranged)
    return f"{98 - int(numeric) % 97:02d}"


def make_iban(country: str, bank_code: str, branch_code: str, account_number: str, rib_key: str) -> str:
    """IBAN au format UEMOA (28 caractères) : CCkk + banque(5) + guichet(5) + compte(12) + clé RIB(2)."""
    bban = f"{bank_code}{branch_code}{account_number}{rib_key}"
    return f"{country}{_iban_check_digits(country, bban)}{bban}"


def is_valid_iban(iban: str) -> bool:
    """Vérifie la clé modulo 97 d'un IBAN (utilisé par les tests)."""
    rearranged = iban[4:] + iban[:4]
    numeric = "".join(str(int(ch, 36)) for ch in rearranged)
    return int(numeric) % 97 == 1
