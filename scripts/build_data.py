#!/usr/bin/env python3
"""
Build an offline Israeli public-transport SQLite database.

All imported GTFS values are stored as TEXT. This is intentional:
GTFS times can exceed 24 hours (for example, 25:10:00).
"""

import argparse
import csv
import io
import re
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path


REQUIRED_GTFS_TABLES = {
    "agency",
    "routes",
    "trips",
    "stops",
    "stop_times",
}

OPTIONAL_GTFS_TABLES = {
    "calendar_dates",
    "calendar",
    "shapes",
    "frequencies",
    "transfers",
    "feed_info",
    "translations",
}


def quote_identifier(value):
    """Safely quote an SQLite table or column name."""
    return '"' + value.replace('"', '""') + '"'


def clean_column_names(columns):
    """Make column names safe and unique without losing their meaning."""
    result = []
    used = set()

    for index, column in enumerate(columns):
        name = (column or "").strip().lstrip("\ufeff")
        if not name:
            name = f"column_{index + 1}"

        name = re.sub(r"[^A-Za-z0-9_]", "_", name)

        if name[0].isdigit():
            name = "_" + name

        original = name
        suffix = 2

        while name.lower() in used:
            name = f"{original}_{suffix}"
            suffix += 1

        used.add(name.lower())
        result.append(name)

    return result


def decode_csv_bytes(data):
    """Decode common encodings used in downloaded CSV files."""
    for encoding in ("utf-8-sig", "utf-8", "cp1255", "windows-1252"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue

    raise ValueError("Could not decode a CSV file using supported encodings")


def find_csv_files(zip_path):
    """Yield CSV/TXT files from a ZIP archive."""
    with zipfile.ZipFile(zip_path, "r") as archive:
        bad_member = archive.testzip()
        if bad_member:
            raise ValueError(f"Damaged ZIP member: {bad_member}")

        for member in archive.namelist():
            if member.endswith("/"):
                continue

            suffix = Path(member).suffix.lower()
            if suffix not in (".txt", ".csv"):
                continue

            yield member, archive.read(member)


def table_name_from_member(member, prefix=""):
    stem = Path(member).stem.lower()
    stem = re.sub(r"[^a-z0-9_]", "_", stem)

    if not stem:
        raise ValueError(f"Invalid table name from file: {member}")

    return prefix + stem


def import_csv(connection, table_name, data):
    """Import one CSV file into a TEXT-only SQLite table."""
    text_data = decode_csv_bytes(data)
    reader = csv.reader(io.StringIO(text_data))

    try:
        headers = next(reader)
    except StopIteration:
        print(f"Skipping empty file: {table_name}")
        return 0

    columns = clean_column_names(headers)

    if not columns:
        raise ValueError(f"No columns found in {table_name}")

    quoted_table = quote_identifier(table_name)
    quoted_columns = ", ".join(quote_identifier(c) for c in columns)

    connection.execute(
        f"DROP TABLE IF EXISTS {quoted_table}"
    )

    definitions = ", ".join(
        f"{quote_identifier(column)} TEXT"
        for column in columns
    )

    connection.execute(
        f"CREATE TABLE {quoted_table} ({definitions})"
    )

    placeholders = ", ".join("?" for _ in columns)

    insert_sql = (
        f"INSERT INTO {quoted_table} ({quoted_columns}) "
        f"VALUES ({placeholders})"
    )

    row_count = 0
    batch = []

    for row in reader:
        if not row or all(value == "" for value in row):
            continue

        if len(row) < len(columns):
            row.extend([""] * (len(columns) - len(row)))
        elif len(row) > len(columns):
            raise ValueError(
                f"Unexpected extra columns in {table_name}, "
                f"row {row_count + 2}"
            )

        batch.append(row)
        row_count += 1

        if len(batch) >= 5000:
            connection.executemany(insert_sql, batch)
            batch.clear()

    if batch:
        connection.executemany(insert_sql, batch)

    print(f"Imported {table_name}: {row_count:,} rows")
    return row_count


def create_indexes(connection):
    """Create useful indexes when the relevant columns exist."""
    indexes = {
        "idx_stops_stop_id": ("stops", ["stop_id"]),
        "idx_stops_stop_name": ("stops", ["stop_name"]),
        "idx_routes_route_id": ("routes", ["route_id"]),
        "idx_routes_route_short_name": ("routes", ["route_short_name"]),
        "idx_routes_agency_id": ("routes", ["agency_id"]),
        "idx_trips_trip_id": ("trips", ["trip_id"]),
        "idx_trips_route_id": ("trips", ["route_id"]),
        "idx_trips_service_id": ("trips", ["service_id"]),
        "idx_stop_times_trip_sequence": (
            "stop_times",
            ["trip_id", "stop_sequence"],
        ),
        "idx_stop_times_stop_id": ("stop_times", ["stop_id"]),
        "idx_calendar_dates_service": (
            "calendar_dates",
            ["service_id", "date"],
        ),
    }

    for index_name, (table, columns) in indexes.items():
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name=?",
            (table,),
        ).fetchone()

        if not table_exists:
            continue

        existing_columns = {
            row[1]
            for row in connection.execute(
                f"PRAGMA table_info({quote_identifier(table)})"
            )
        }

        if all(column in existing_columns for column in columns):
            column_sql = ", ".join(
                quote_identifier(column) for column in columns
            )

            connection.execute(
                f"CREATE INDEX IF NOT EXISTS "
                f"{quote_identifier(index_name)} "
                f"ON {quote_identifier(table)} ({column_sql})"
            )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--gtfs", required=True)
    parser.add_argument("--networks", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    gtfs_path = Path(args.gtfs)
    networks_path = Path(args.networks)
    output_path = Path(args.output)

    if not gtfs_path.is_file():
        raise FileNotFoundError(f"GTFS ZIP not found: {gtfs_path}")

    if not networks_path.is_file():
        raise FileNotFoundError(
            f"Networks ZIP not found: {networks_path}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        output_path.unlink()

    connection = sqlite3.connect(output_path)

    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")

        connection.execute("""
            CREATE TABLE import_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        imported_gtfs = set()
        imported_networks = set()

        print("Importing main GTFS archive...")

        for member, data in find_csv_files(gtfs_path):
            table_name = table_name_from_member(member)

            if table_name in imported_gtfs:
                raise ValueError(
                    f"Duplicate GTFS table name: {table_name}"
                )

            import_csv(connection, table_name, data)
            imported_gtfs.add(table_name)

        missing = REQUIRED_GTFS_TABLES - imported_gtfs

        if missing:
            raise ValueError(
                "Missing required GTFS tables: "
                + ", ".join(sorted(missing))
            )

        print("Importing route-network archive...")

        for member, data in find_csv_files(networks_path):
            table_name = table_name_from_member(
                member,
                prefix="networks_",
            )

            if table_name in imported_networks:
                raise ValueError(
                    f"Duplicate networks table name: {table_name}"
                )

            import_csv(connection, table_name, data)
            imported_networks.add(table_name)

        if "calendar_dates" not in imported_gtfs:
            print(
                "WARNING: calendar_dates.txt was not found. "
                "Check the current official feed specification."
            )

        metadata = {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "gtfs_source": gtfs_path.name,
            "networks_source": networks_path.name,
            "gtfs_tables": ",".join(sorted(imported_gtfs)),
            "networks_tables": ",".join(sorted(imported_networks)),
            "format_version": "1",
        }

        connection.executemany(
            "INSERT INTO import_metadata (key, value) VALUES (?, ?)",
            metadata.items(),
        )

        create_indexes(connection)
        connection.commit()

        integrity = connection.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]

        if integrity != "ok":
            raise RuntimeError(
                f"SQLite integrity check failed: {integrity}"
            )

        print("SQLite integrity check: OK")
        print(f"Database created: {output_path}")
        print(f"Database size: {output_path.stat().st_size:,} bytes")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"BUILD FAILED: {error}", file=sys.stderr)
        sys.exit(1)
