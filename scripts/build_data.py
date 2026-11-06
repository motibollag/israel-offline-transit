#!/usr/bin/env python3
"""
Build an offline Israeli public-transport SQLite database.

All source files, tables and columns are imported.
Only selected fields are stored using compact numeric types:

stop_times.arrival_time       -> INTEGER seconds from service-day start
stop_times.departure_time     -> INTEGER seconds from service-day start
stop_times.stop_sequence      -> INTEGER
shapes.shape_pt_sequence      -> INTEGER

All identifiers, names, coordinates and other values remain TEXT.

GTFS times above 24:00 are preserved as seconds from the start
of the service day. Example: 25:10:00 = 90600 seconds.
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

# Only these explicitly understood fields are converted.
# All other fields, including fields added in future feeds, remain TEXT.
INTEGER_COLUMNS = {
    ("stop_times", "stop_sequence"),
    ("shapes", "shape_pt_sequence"),
    ("stop_times", "arrival_time"),
    ("stop_times", "departure_time"),
}

TIME_COLUMNS = {
    ("stop_times", "arrival_time"),
    ("stop_times", "departure_time"),
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
    for encoding in (
        "utf-8-sig",
        "utf-8",
        "cp1255",
        "windows-1252",
    ):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue

    raise ValueError(
        "Could not decode a CSV file using supported encodings"
    )


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
        raise ValueError(
            f"Invalid table name from file: {member}"
        )

    return prefix + stem


def parse_gtfs_time(value, table_name, column_name, row_number):
    """
    Convert GTFS HH:MM:SS or H:MM:SS to seconds.

    Hours above 24 are deliberately supported.
    Empty values remain empty strings.
    """
    if value == "":
        return ""

    match = re.fullmatch(
        r"([0-9]{1,3}):([0-5][0-9]):([0-5][0-9])",
        value.strip(),
    )

    if not match:
        raise ValueError(
            f"Invalid GTFS time in {table_name}.{column_name}, "
            f"row {row_number}: {value!r}"
        )

    hours = int(match.group(1))
    minutes = int(match.group(2))
    seconds = int(match.group(3))

    return hours * 3600 + minutes * 60 + seconds


def parse_nonnegative_integer(
    value,
    table_name,
    column_name,
    row_number,
):
    """Convert an integer field without silently changing its value."""
    if value == "":
        return ""

    if not re.fullmatch(r"[0-9]+", value.strip()):
        raise ValueError(
            f"Invalid non-negative integer in "
            f"{table_name}.{column_name}, "
            f"row {row_number}: {value!r}"
        )

    return int(value)


def convert_value(table_name, column_name, value, row_number):
    """Convert only fields explicitly listed above."""
    key = (table_name, column_name.lower())

    if key in TIME_COLUMNS:
        return parse_gtfs_time(
            value,
            table_name,
            column_name,
            row_number,
        )

    if key in INTEGER_COLUMNS:
        return parse_nonnegative_integer(
            value,
            table_name,
            column_name,
            row_number,
        )

    return value


def import_csv(connection, table_name, data):
    """Import one CSV file, preserving every source column."""
    text_data = decode_csv_bytes(data)
    reader = csv.reader(io.StringIO(text_data, newline=""))

    try:
        headers = next(reader)
    except StopIteration:
        print(f"Skipping empty file: {table_name}")
        return 0

    columns = clean_column_names(headers)

    if not columns:
        raise ValueError(f"No columns found in {table_name}")

    quoted_table = quote_identifier(table_name)
    quoted_columns = ", ".join(
        quote_identifier(column) for column in columns
    )

    definitions = []

    for column in columns:
        key = (table_name, column.lower())

        if key in INTEGER_COLUMNS:
            column_type = "INTEGER"
        else:
            column_type = "TEXT"

        definitions.append(
            f"{quote_identifier(column)} {column_type}"
        )

    connection.execute(
        f"DROP TABLE IF EXISTS {quoted_table}"
    )

    connection.execute(
        f"CREATE TABLE {quoted_table} "
        f"({', '.join(definitions)})"
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

        converted_row = []

        for index, value in enumerate(row):
            converted_row.append(
                convert_value(
                    table_name,
                    columns[index],
                    value,
                    row_count + 2,
                )
            )

        batch.append(converted_row)
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
        "idx_stops_stop_id": (
            "stops",
            ["stop_id"],
        ),
        "idx_stops_stop_name": (
            "stops",
            ["stop_name"],
        ),
        "idx_routes_route_id": (
            "routes",
            ["route_id"],
        ),
        "idx_routes_route_short_name": (
            "routes",
            ["route_short_name"],
        ),
        "idx_routes_agency_id": (
            "routes",
            ["agency_id"],
        ),
        "idx_trips_trip_id": (
            "trips",
            ["trip_id"],
        ),
        "idx_trips_route_id": (
            "trips",
            ["route_id"],
        ),
        "idx_trips_service_id": (
            "trips",
            ["service_id"],
        ),
        "idx_stop_times_trip_sequence": (
            "stop_times",
            ["trip_id", "stop_sequence"],
        ),
        "idx_stop_times_stop_id": (
            "stop_times",
            ["stop_id"],
        ),
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
                quote_identifier(column)
                for column in columns
            )

            connection.execute(
                f"CREATE INDEX IF NOT EXISTS "
                f"{quote_identifier(index_name)} "
                f"ON {quote_identifier(table)} ({column_sql})"
            )


def verify_converted_columns(connection, imported_tables):
    """Verify that converted columns have the expected SQLite types."""
    expected = {
        "stop_times": {
            "arrival_time",
            "departure_time",
            "stop_sequence",
        },
        "shapes": {
            "shape_pt_sequence",
        },
    }

    for table, required_columns in expected.items():
        if table not in imported_tables:
            continue

        column_info = {
            row[1]: row[2].upper()
            for row in connection.execute(
                f"PRAGMA table_info({quote_identifier(table)})"
            )
        }

        for column in required_columns:
            if column not in column_info:
                continue

            if column_info[column] != "INTEGER":
                raise RuntimeError(
                    f"Unexpected type for {table}.{column}: "
                    f"{column_info[column]}"
                )

    # Verify that converted time values are actually stored as integers.
    if "stop_times" in imported_tables:
        for column in ("arrival_time", "departure_time"):
            result = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM stop_times
                WHERE {quote_identifier(column)} != ''
                  AND typeof({quote_identifier(column)}) != 'integer'
                """
            ).fetchone()[0]

            if result:
                raise RuntimeError(
                    f"{result:,} non-integer values found in "
                    f"stop_times.{column}"
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
        raise FileNotFoundError(
            f"GTFS ZIP not found: {gtfs_path}"
        )

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
                "NOTICE: calendar_dates.txt was not found. "
                "The imported calendar table, if present, "
                "has been preserved. Check the current feed "
                "specification before interpreting service dates."
            )

        metadata = {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "gtfs_source": gtfs_path.name,
            "networks_source": networks_path.name,
            "gtfs_tables": ",".join(sorted(imported_gtfs)),
            "networks_tables": ",".join(sorted(imported_networks)),
            "format_version": "2",
            "time_encoding": "seconds_since_service_day_start",
            "time_columns": (
                "stop_times.arrival_time,"
                "stop_times.departure_time"
            ),
            "integer_columns": (
                "stop_times.stop_sequence,"
                "shapes.shape_pt_sequence"
            ),
            "source_columns_preserved": "true",
            "source_tables_preserved": "true",
        }

        connection.executemany(
            "INSERT INTO import_metadata (key, value) "
            "VALUES (?, ?)",
            metadata.items(),
        )

        create_indexes(connection)

        verify_converted_columns(
            connection,
            imported_gtfs,
        )

        connection.commit()

        integrity = connection.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]

        if integrity != "ok":
            raise RuntimeError(
                f"SQLite integrity check failed: {integrity}"
            )

        print("SQLite integrity check: OK")
        print("Converted-column verification: OK")
        print(f"Database created: {output_path}")
        print(
            f"Database size: "
            f"{output_path.stat().st_size:,} bytes"
        )

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(
            f"BUILD FAILED: {error}",
            file=sys.stderr,
        )
        sys.exit(1)
