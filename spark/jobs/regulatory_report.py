"""Niveau 2 — Rapport réglementaire quotidien J+1 (BCEAO / CIMA).

Planifié par Airflow à 00h30 UTC, il produit les agrégats de la veille (J-1) :
- table gold.regulatory_daily_report (partitionnée par date de rapport et pays) ;
- export CSV dans MinIO : s3a://lakehouse/reports/regulatory/report_date=AAAA-MM-JJ/country_code=XX/

Usage : spark-submit regulatory_report.py --report-date 2026-09-30 [--country ALL]
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import transforms_gold as G
from common.logging_utils import get_logger
from common.session import get_spark
from common.tables import filter_countries, fq, overwrite_partitions, parse_countries

log = get_logger("regulatory_report")
REPORTS_PATH = os.environ.get("REGULATORY_REPORTS_PATH", "s3a://lakehouse/reports/regulatory")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Rapport réglementaire J-1")
    p.add_argument("--report-date", default=(date.today() - timedelta(days=1)).isoformat(),
                   help="Jour couvert par le rapport (AAAA-MM-JJ), défaut : hier")
    p.add_argument("--country", default="ALL")
    args = p.parse_args(argv)
    report_date = date.fromisoformat(args.report_date).isoformat()   # validation du format
    countries = parse_countries(args.country)

    spark = get_spark("regulatory_report")
    # N'écrase que les partitions (date, pays) produites, pas tout le dossier d'export
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        bank = filter_countries(spark.table(fq("silver", "bank_transactions")), countries)
        ins = filter_countries(spark.table(fq("silver", "insurance_operations")), countries)
        npl = filter_countries(spark.table(fq("gold", "npl_ratio_by_country")), countries)
        report = (G.regulatory_daily_report(report_date, bank, npl, ins)
                  .withColumn("_generated_at",
                              F.lit(datetime.now(timezone.utc).replace(tzinfo=None)).cast("timestamp")))
        rows = overwrite_partitions(spark, report, fq("gold", "regulatory_daily_report"),
                                    ["report_date", "country_code"], "Agregats reglementaires J+1 BCEAO / CIMA")
        (report.coalesce(1).write.mode("overwrite").option("header", "true")
               .partitionBy("report_date", "country_code").csv(REPORTS_PATH))
        log.info("rapport réglementaire produit", extra={"ctx": {
            "report_date": report_date, "rows": rows, "countries": countries, "export": REPORTS_PATH}})
        return 0
    except Exception:
        log.exception("échec du rapport réglementaire", extra={"ctx": {"report_date": report_date}})
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
