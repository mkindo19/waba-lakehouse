"""Tests de la supervision des jobs streaming (common/stream_monitor.py)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "jobs"))

from common.stream_monitor import group_id, parse_kafka_offsets  # noqa: E402


def test_parse_kafka_offsets():
    assert parse_kafka_offsets('{"silver-bank-transactions":{"0":1234,"1":7}}') == \
        {"silver-bank-transactions": {0: 1234, 1: 7}}


def test_parse_ignores_non_kafka_and_negative_offsets():
    assert parse_kafka_offsets("42") == {}                    # source « rate » : offset numérique
    assert parse_kafka_offsets(None) == {}
    assert parse_kafka_offsets("pas du json") == {}
    assert parse_kafka_offsets('{"t":{"0":-1}}') == {"t": {}}  # -1 / -2 = earliest / latest symboliques


def test_group_id():
    assert group_id("fraud_aml_aml") == "waba-spark.fraud_aml_aml"
