#!/usr/bin/env python3
"""Migration du catalogue Iceberg REST : SQLite -> PostgreSQL (niveau 4, cf. D31).

Exécuté par l'initContainer du pod iceberg-rest, AVANT le démarrage du serveur :
1. crée les tables du catalogue JDBC d'Iceberg dans PostgreSQL (schéma « V1 » d'Iceberg 1.6, identique à celui
   que le serveur créerait lui-même) ;
2. si elles sont vides et qu'un ancien catalogue SQLite existe, y recopie toutes ses lignes : chaque table Iceberg
   garde son pointeur vers son fichier de métadonnées dans MinIO -> aucune donnée à réécrire ni à réenregistrer.
Idempotent : dès que PostgreSQL contient des tables, la migration n'est plus refaite. Le fichier SQLite n'est pas
modifié (retour arrière possible).
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time

import psycopg2

SQLITE_PATH = os.environ.get("SQLITE_CATALOG", "/catalog/iceberg_catalog.db")
DDL = {
    "iceberg_tables": """CREATE TABLE IF NOT EXISTS iceberg_tables (
        catalog_name VARCHAR(255) NOT NULL, table_namespace VARCHAR(255) NOT NULL,
        table_name VARCHAR(255) NOT NULL, metadata_location VARCHAR(1000),
        previous_metadata_location VARCHAR(1000), iceberg_type VARCHAR(5),
        PRIMARY KEY (catalog_name, table_namespace, table_name))""",
    "iceberg_namespace_properties": """CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
        catalog_name VARCHAR(255) NOT NULL, namespace VARCHAR(255) NOT NULL,
        property_key VARCHAR(255), property_value VARCHAR(1000),
        PRIMARY KEY (catalog_name, namespace, property_key))""",
}


def connect_pg(retries: int = 60):
    for i in range(retries):
        try:
            return psycopg2.connect(host=os.environ.get("PGHOST", "iceberg-catalog-db"),
                                    port=int(os.environ.get("PGPORT", "5432")),
                                    dbname=os.environ.get("PGDATABASE", "iceberg"),
                                    user=os.environ.get("PGUSER", "iceberg"),
                                    password=os.environ["ICEBERG_CATALOG_DB_PASSWORD"])
        except psycopg2.OperationalError:
            print(f"attente de PostgreSQL ({i + 1}/{retries})...", flush=True)
            time.sleep(5)
    sys.exit("PostgreSQL injoignable")


def main() -> int:
    pg = connect_pg()
    with pg, pg.cursor() as cur:
        for ddl in DDL.values():
            cur.execute(ddl)
        cur.execute("SELECT count(*) FROM iceberg_tables")
        existing = cur.fetchone()[0]
    if existing:
        print(f"catalogue PostgreSQL déjà en place ({existing} tables Iceberg) : rien à migrer")
        return 0
    if not os.path.exists(SQLITE_PATH):
        print("aucun ancien catalogue SQLite : démarrage sur un catalogue vide")
        return 0

    src = sqlite3.connect(SQLITE_PATH)
    with pg, pg.cursor() as cur:
        for table in DDL:
            src_cols = [r[1] for r in src.execute(f"PRAGMA table_info({table})")]
            if not src_cols:
                print(f"{table} : absente du catalogue SQLite")
                continue
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,))
            dst_cols = {r[0] for r in cur.fetchall()}
            cols = [c for c in src_cols if c in dst_cols]
            rows = src.execute(f"SELECT {', '.join(cols)} FROM {table}").fetchall()
            placeholders = ", ".join(["%s"] * len(cols))
            cur.executemany(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})", rows)
            print(f"{table} : {len(rows)} ligne(s) recopiée(s) depuis SQLite")
    src.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
