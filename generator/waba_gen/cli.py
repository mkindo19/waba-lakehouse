"""CLI du générateur (utile pour l'automatisation, les tests et Airflow).

Exemples :
    python -m waba_gen referentials --scale 0.1
    python -m waba_gen batch --datasets bank_transactions mobile_money_payments --countries CI SN
    python -m waba_gen stream --duration 300
    python -m waba_gen batch --local ./out     # écrit sur disque au lieu de MinIO
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from . import config as cfg
from .engine import ContinuousRunner, load_referentials, publish_referentials, run_continuous_blocking, run_one_time
from .storage import LocalSink, S3Sink, Sink
from .transactions import RefIndex
from .utils import get_logger

log = get_logger("waba_gen.cli")


def last_quarter(today: date | None = None) -> tuple[date, date]:
    """Trimestre civil précédent (valeur par défaut de la période simulée)."""
    today = today or date.today()
    q_start_month = 3 * ((today.month - 1) // 3) + 1
    current_q_start = date(today.year, q_start_month, 1)
    end = current_q_start - timedelta(days=1)
    start_month = 3 * ((end.month - 1) // 3) + 1
    return date(end.year, start_month, 1), end


def _sink(args: argparse.Namespace) -> Sink:
    return LocalSink(args.local) if args.local else S3Sink()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="waba_gen", description="Générateur de données WABA Group")
    parser.add_argument("--local", help="Écrire dans ce dossier local au lieu de MinIO")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ref = sub.add_parser("referentials", help="Générer et publier les référentiels")
    p_ref.add_argument("--seed", type=int, default=42)
    p_ref.add_argument("--scale", type=float, default=1.0, help="1.0 = 500k clients / 800k comptes")

    for name in ("batch", "stream"):
        p = sub.add_parser(name)
        p.add_argument("--datasets", nargs="+", default=list(cfg.DATASETS), choices=list(cfg.DATASETS))
        p.add_argument("--countries", nargs="+", default=cfg.COUNTRIES, choices=cfg.COUNTRIES)
        p.add_argument("--entities", nargs="+", default=cfg.ENTITY_TYPES, choices=cfg.ENTITY_TYPES)
        p.add_argument("--seed", type=int, default=None)

    p_batch = sub.choices["batch"]
    start, end = last_quarter()
    p_batch.add_argument("--start", type=date.fromisoformat, default=start)
    p_batch.add_argument("--end", type=date.fromisoformat, default=end)
    for ds, n in cfg.DEFAULT_ROWS.items():
        p_batch.add_argument(f"--rows-{ds.replace('_', '-')}", type=int, default=n, dest=f"rows_{ds}")

    p_stream = sub.choices["stream"]
    p_stream.add_argument("--rows-per-tick", type=int, default=50)
    p_stream.add_argument("--min-interval", type=int, default=10)
    p_stream.add_argument("--max-interval", type=int, default=60)
    p_stream.add_argument("--duration", type=int, default=None, help="Durée en secondes (défaut : infini)")

    args = parser.parse_args(argv)
    sink = _sink(args)

    if args.cmd == "referentials":
        publish_referentials(sink, seed=args.seed, scale=args.scale)
        return

    idx = RefIndex.build(load_referentials(sink))
    if args.cmd == "batch":
        rows = {ds: getattr(args, f"rows_{ds}") for ds in cfg.DATASETS}
        keys = run_one_time(sink, idx, args.datasets, rows, args.countries, args.entities, args.start, args.end, args.seed)
        log.info("génération one-time terminée", extra={"ctx": {"files": len(keys)}})
    else:
        runner = ContinuousRunner(
            sink, idx, args.datasets, {ds: args.rows_per_tick for ds in cfg.DATASETS},
            args.countries, args.entities, args.min_interval, args.max_interval, args.seed,
        )
        run_continuous_blocking(runner, args.duration)


if __name__ == "__main__":
    main()
