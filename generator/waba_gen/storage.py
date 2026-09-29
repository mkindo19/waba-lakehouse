"""Écriture des fichiers générés : MinIO (S3) ou disque local (tests / hors Docker).

Arborescence dans le bucket raw-landing (organisée par type puis pays) :
    referentials/customers/customers.csv
    bank_transactions/CI/bank_txn_CI_20260101_01.csv
    mobile_money_payments/SN/mobile_money_SN_20260101_03.csv
"""

from __future__ import annotations

import io
import os
import re
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

import pandas as pd

from . import config as cfg
from .utils import get_logger

log = get_logger(__name__)

_SEQ_RE = re.compile(r"_(\d{8})_(\d+)\.csv$")


def referential_key(name: str) -> str:
    return f"referentials/{name}/{name}.csv"


def dataset_prefix(dataset: str, country: str) -> str:
    return f"{cfg.DATASETS[dataset]['folder']}/{country}/"


def dataset_filename(dataset: str, country: str, day: date, seq: int) -> str:
    prefix = cfg.DATASETS[dataset]["file_prefix"]
    return f"{prefix}_{country}_{day:%Y%m%d}_{seq:02d}.csv"


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


class Sink(ABC):
    """Destination des fichiers. Gère la numérotation NN sans écraser l'existant."""

    @abstractmethod
    def put(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def get(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool:
        return key in self.list_keys(key)

    def next_seq(self, dataset: str, country: str, day: date) -> int:
        """Prochain numéro NN libre pour (dataset, pays, jour) : jamais d'écrasement."""
        stem = f"{cfg.DATASETS[dataset]['file_prefix']}_{country}_{day:%Y%m%d}_"
        used = [
            int(m.group(2))
            for k in self.list_keys(dataset_prefix(dataset, country) + stem)
            if (m := _SEQ_RE.search(k))
        ]
        return max(used, default=0) + 1

    def write_batch(self, dataset: str, country: str, day: date, df: pd.DataFrame) -> str:
        key = dataset_prefix(dataset, country) + dataset_filename(dataset, country, day, self.next_seq(dataset, country, day))
        self.put(key, to_csv_bytes(df))
        log.info("fichier déposé", extra={"ctx": {"key": key, "rows": len(df), "dataset": dataset, "country": country}})
        return key

    def write_referential(self, name: str, df: pd.DataFrame) -> str:
        key = referential_key(name)
        self.put(key, to_csv_bytes(df))
        log.info("référentiel déposé", extra={"ctx": {"key": key, "rows": len(df)}})
        return key

    def read_referential(self, name: str) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(self.get(referential_key(name))), keep_default_na=True)


class S3Sink(Sink):
    """MinIO via l'API S3 (boto3). Identifiants lus dans l'environnement uniquement."""

    def __init__(self, bucket: str | None = None) -> None:
        import boto3  # import local : inutile pour les tests en mode fichier
        from botocore.config import Config

        self.bucket = bucket or os.environ.get("RAW_BUCKET", "raw-landing")
        self.client = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}),
        )

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType="text/csv")

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys


class LocalSink(Sink):
    """Même arborescence, sur disque local."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def put(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get(self, key: str) -> bytes:
        return (self.root / key).read_bytes()

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root / Path(prefix).parent if not prefix.endswith("/") else self.root / prefix
        if not base.exists():
            return []
        keys = (p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file())
        return [k for k in keys if k.startswith(prefix)]
