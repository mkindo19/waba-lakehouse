"""Couche service : générer et publier (one-time ou continu). Utilisée par Streamlit et la CLI."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

import numpy as np

from . import config as cfg
from .referentials import Referentials, generate_referentials
from .storage import Sink
from .transactions import RefIndex, generate_dataset
from .utils import get_logger

log = get_logger(__name__)


def publish_referentials(sink: Sink, seed: int = 42, scale: float = 1.0) -> Referentials:
    """Génère les référentiels et les dépose (à faire AVANT toute transaction)."""
    refs = generate_referentials(seed=seed, scale=scale)
    for name, df in refs.as_dict().items():
        sink.write_referential(name, df)
    return refs


def load_referentials(sink: Sink) -> Referentials:
    """Relit les référentiels déjà publiés : les transactions ne référencent qu'eux."""
    try:
        frames = {name: sink.read_referential(name) for name in cfg.REFERENTIALS}
    except Exception as exc:  # fichier absent, MinIO indisponible...
        raise RuntimeError(
            "Référentiels introuvables : générez d'abord customers/accounts/branches/products."
        ) from exc
    return Referentials(**frames)


def run_one_time(
    sink: Sink,
    idx: RefIndex,
    datasets: list[str],
    rows: dict[str, int],
    countries: list[str],
    entity_types: list[str],
    start: date,
    end: date,
    seed: int | None = None,
) -> list[str]:
    if start > end:
        raise ValueError("La date de début doit précéder la date de fin")
    rng = np.random.default_rng(seed)
    keys: list[str] = []
    for ds in datasets:
        for country, day, df in generate_dataset(ds, idx, rows[ds], countries, entity_types, start, end, rng):
            keys.append(sink.write_batch(ds, country, day, df))
    return keys


def run_tick(
    sink: Sink,
    idx: RefIndex,
    datasets: list[str],
    rows_per_tick: dict[str, int],
    countries: list[str],
    entity_types: list[str],
    rng: np.random.Generator,
) -> list[str]:
    """Un "battement" du mode continu : un petit lot horodaté aujourd'hui (UTC)."""
    today = datetime.now(timezone.utc).date()
    keys: list[str] = []
    for ds in datasets:
        for country, day, df in generate_dataset(ds, idx, rows_per_tick[ds], countries, entity_types, today, today, rng):
            keys.append(sink.write_batch(ds, country, day, df))
    return keys


@dataclass
class ContinuousRunner:
    """Génération continue dans un thread : un lot toutes les 10 à 60 secondes."""

    sink: Sink
    idx: RefIndex
    datasets: list[str]
    rows_per_tick: dict[str, int]
    countries: list[str]
    entity_types: list[str]
    min_interval: int = 10
    max_interval: int = 60
    seed: int | None = None
    ticks: int = 0
    files: int = 0
    last_error: str | None = None
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)

    def start(self) -> None:
        if self.is_running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="waba-continuous", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _loop(self) -> None:
        rng = np.random.default_rng(self.seed)
        log.info("mode continu démarré", extra={"ctx": {"datasets": self.datasets, "countries": self.countries}})
        while not self._stop.is_set():
            try:
                keys = run_tick(self.sink, self.idx, self.datasets, self.rows_per_tick,
                                self.countries, self.entity_types, rng)
                self.ticks += 1
                self.files += len(keys)
                self.last_error = None
            except Exception as exc:  # on journalise et on continue : le flux ne doit pas mourir
                self.last_error = str(exc)
                log.exception("échec d'un lot continu")
            self._stop.wait(int(rng.integers(self.min_interval, self.max_interval + 1)))
        log.info("mode continu arrêté", extra={"ctx": {"ticks": self.ticks, "files": self.files}})


def run_continuous_blocking(runner: ContinuousRunner, duration_s: int | None = None) -> None:
    """Version CLI : tourne jusqu'à Ctrl+C ou jusqu'à `duration_s` secondes."""
    runner.start()
    started = time.monotonic()
    try:
        while runner.is_running and (duration_s is None or time.monotonic() - started < duration_s):
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        runner.stop()
