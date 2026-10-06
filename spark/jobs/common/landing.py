"""Accès au bucket raw-landing (listing) et archivage sécurisé des fichiers traités."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from .logging_utils import get_logger

log = get_logger(__name__)

RAW_BUCKET = os.environ.get("RAW_BUCKET", "raw-landing")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET", "archive")
# Niveau 3 : NiFi lit aussi raw-landing (ListS3 toutes les ~10 s). On n'archive qu'un fichier
# déposé depuis au moins N secondes, pour laisser à NiFi le temps de le lire (cf. D20).
ARCHIVE_MIN_AGE_SECONDS = int(os.environ.get("ARCHIVE_MIN_AGE_SECONDS", "120"))


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}),
    )


def list_landing_files(prefix: str, suffix: str = ".csv") -> list[str]:
    """Clés des fichiers en attente sous `prefix` dans raw-landing."""
    client = s3_client()
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        keys.extend(o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(suffix))
    return sorted(keys)


def to_s3a(key: str, bucket: str = RAW_BUCKET) -> str:
    return f"s3a://{bucket}/{key}"


def archive_files(keys: list[str], min_age_seconds: int | None = None) -> int:
    """Déplace les fichiers vers le bucket archive (copie vérifiée PUIS suppression).

    Opération non destructive : la source n'est supprimée que si la copie existe
    avec la même taille. En cas d'échec, le fichier reste dans raw-landing et
    sera simplement re-traité (sans doublon grâce au MERGE).

    Un fichier trop récent (moins de `min_age_seconds`) est laissé en place : il sera
    archivé au passage suivant, après avoir été lu par NiFi (niveau 3).
    """
    min_age = ARCHIVE_MIN_AGE_SECONDS if min_age_seconds is None else min_age_seconds
    client = s3_client()
    moved = too_recent = 0
    now = datetime.now(timezone.utc)
    for key in keys:
        try:
            src = client.head_object(Bucket=RAW_BUCKET, Key=key)
            if (now - src["LastModified"]).total_seconds() < min_age:
                too_recent += 1
                continue
            client.copy_object(Bucket=ARCHIVE_BUCKET, Key=key, CopySource={"Bucket": RAW_BUCKET, "Key": key})
            dst = client.head_object(Bucket=ARCHIVE_BUCKET, Key=key)
            if dst["ContentLength"] != src["ContentLength"]:
                raise RuntimeError("taille différente après copie")
            client.delete_object(Bucket=RAW_BUCKET, Key=key)
            moved += 1
        except Exception:
            log.exception("archivage impossible, fichier conservé", extra={"ctx": {"key": key}})
    log.info("fichiers archivés", extra={"ctx": {"moved": moved, "too_recent": too_recent, "total": len(keys)}})
    return moved
