#!/usr/bin/env python3
"""
Build an optimized, high-performance offline Israeli public-transport SQLite database.

1. Compact ID Encoding: Maps long text trip_ids (25+ characters) to compact
   sequential integers (1, 2, 3...), slashing table and index sizes by hundreds of MBs.
2. Filter active schedules only (from yesterday to +35 days), eliminating millions of dead rows.
3. Clustered primary keys on stop_times (trip_id, stop_sequence) for instant query speed.
"""

import argparse
import csv
import io
import re
import sqlite3
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


ALLOWED_GTFS_TABLES = {
    "agency",
    "routes",
    "trips",
    "stops",
    "stop_times",
    "calendar",
    "calendar_dates",
}

REQUIRED_GTFS_TABLES = {
    "agency",
    "routes",
    "trips",
    "stops",
    "stop_times",
    "calendar",
}

STOP_TIMES_COLUMNS = {
    "trip_id",
    "arrival_time",
    "departure_time",
    "stop_id",
    "stop_sequence",
}

INTEGER_COLUMNS = {
    ("stop_times", "trip_id"),
    ("stop_times", "stop_sequence"),
    ("stop_times", "arrival_time"),
    ("stop_times", "departure_time"),
    ("trips", "trip_id"),
}

TIME_COLUMNS = {
    ("stop_times", "arrival_time"),
    ("stop_times", "departure_time"),
}


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def clean_column_names(columns):
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
    for encoding in ("utf-8-sig", "utf-8", "cp1255", "windows-1252"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("Could not decode CSV file using supported encodings")


def parse_gtfs_time(value, table_name, column_name, row_number):
    if value == "" or value is None:
        return None

    match = re.fullmatch(r"([0-9]{1,3}):([0-5][0-9]):([0-5][0-9])", str(value).strip())
    if not match:
        raise ValueError(f"Invalid GTFS time in {table_name}.{column_name}, row {row_number}: {value!r}")

    hours = int(match.group(1))
    minutes = int(match.group(2))
    seconds = int(match.group(3))
    return hours * 3600 + minutes * 60 + seconds


def parse_nonnegative_integer(value, table_name, column_name, row_number):
    if value == "" or value is None:
        return None

    if isinstance(value, int):
        return value

    if not re.fullmatch(r"[0-9]+", str(value).strip()):
        raise ValueError(f"Invalid integer in {table_name}.{column_name}, row {row_number}: {value!r}")

    return int(value)


def convert_value(table_name, column_name, value, row_number):
    key = (table_name, column_name.lower())
    if key in TIME_COLUMNS:
        return parse_gtfs_time(value, table_name, column_name, row_number)
    if key in INTEGER_COLUMNS:
        return parse_nonnegative_integer(value, table_name, column_name, row_number)
    return value if (value != "" and value is not None) else ""


def find_archive_member(archive, target_name):
    target = target_name.lower()
    for name in archive.namelist():
        if Path(name).name.lower() == target:
            return name
    return None


def collect_active_services_and_map_trips(archive):
    """
    1. Filter active calendar service_ids.
    2. Map long trip_id strings to compact integer codes (1, 2, 3...).
    """
    today = datetime.now(timezone.utc).date()
    min_date_str = (today - timedelta(days=1)).strftime("%Y%m%d")
    max_date_str = (today + timedelta(days=35)).strftime("%Y%m%d")

    print(f"Date window for active schedules: {min_date_str} to {max_date_str}")
    active_service_ids = set()

    calendar_member = find_archive_member(archive, "calendar.txt")
    if calendar_member:
        text_data = decode_csv_bytes(archive.read(calendar_member))
        reader = csv.DictReader(io.StringIO(text_data))
        for row in reader:
            s_id = row.get("service_id", "").strip()
            start = row.get("start_date", "").strip()
            end = row.get("end_date", "").strip()
            if s_id and end >= min_date_str and start <= max_date_str:
                active_service_ids.add(s_id)

    dates_member = find_archive_member(archive, "calendar_dates.txt")
    if dates_member:
        text_data = decode_csv_bytes(archive.read(dates_member))
        reader = csv.DictReader(io.StringIO(text_data))
        for row in reader:
            s_id = row.get("service_id", "").strip()
            d = row.get("date", "").strip()
            if s_id and min_date_str <= d <= max_date_str:
                active_service_ids.add(s_id)

    print(f"Active service IDs identified: {len(active_service_ids):,}")
    filter_by_service = len(active_service_ids) > 0

    trip_id_to_int = {}
    trips_member = find_archive_member(archive, "trips.txt")

    if trips_member:
        text_data = decode_csv_bytes(archive.read(trips_member))
        reader = csv.DictReader(io.StringIO(text_data))
        next_int_id = 1
        for row in reader:
            t_id = row.get("trip_id", "").strip()
            s_id = row.get("service_id", "").strip()
            if not t_id:
                continue

            # Keep only active trips if services were filtered
            if filter_by_service and s_id not in active_service_ids:
                continue

            if t_id not in trip_id_to_int:
                trip_id_to_int[t_id] = next_int_id
                next_int_id += 1

    print(f"Active trips mapped to compact integer IDs: {len(trip_id_to_int):,}")
    return active_service_ids if filter_by_service else None, trip_id_to_int


def import_csv(connection, table_name, data, active_service_ids, trip_id_to_int):
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

    service_id_idx = None
    trip_id_idx = None
    for idx, col in enumerate(columns):
        if col.lower() == "service_id":
            service_id_idx = idx
        if col.lower() == "trip_id":
            trip_id_idx = idx

    included_indices = []
    final_columns = []

    for idx, col in enumerate(columns):
        if table_name == "stop_times" and col.lower() not in STOP_TIMES_COLUMNS:
            continue
        included_indices.append(idx)
        final_columns.append(col)

    quoted_table = quote_identifier(table_name)
    quoted_columns = ", ".join(quote_identifier(col) for col in final_columns)

    definitions = []
    for column in final_columns:
        key = (table_name, column.lower())
        column_type = "INTEGER" if key in INTEGER_COLUMNS else "TEXT"
        definitions.append(f"{quote_identifier(column)} {column_type}")

    connection.execute(f"DROP TABLE IF EXISTS {quoted_table}")

    # Clustered primary key on stop_times eliminates the need for separate index
    if table_name == "stop_times":
        definitions.append("PRIMARY KEY (\"trip_id\", \"stop_sequence\")")
        connection.execute(f"CREATE TABLE {quoted_table} ({', '.join(definitions)}) WITHOUT ROWID")
    else:
        connection.execute(f"CREATE TABLE {quoted_table} ({', '.join(definitions)})")

    placeholders = ", ".join("?" for _ in final_columns)
    insert_sql = f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"

    row_count = 0
    batch = []

    for row in reader:
        if not row or all(v == "" for v in row):
            continue

        if len(row) < len(columns):
            row.extend([""] * (len(columns) - len(row)))

        if table_name in ("calendar", "calendar_dates") and active_service_ids is not None:
            if service_id_idx is not None and row[service_id_idx].strip() not in active_service_ids:
                continue

        # In trips and stop_times, replace string trip_id with compact integer ID
        if table_name in ("trips", "stop_times") and trip_id_idx is not None:
            raw_trip_id = row[trip_id_idx].strip()
            int_id = trip_id_to_int.get(raw_trip_id)
            if int_id is None:
                continue  # Skip inactive trip
            row[trip_id_idx] = int_id

        converted_row = []
        for idx in included_indices:
            converted_row.append(
                convert_value(
                    table_name,
                    columns[idx],
                    row[idx],
                    row_count + 2
                )
            )

        batch.append(converted_row)
        row_count += 1

        if len(batch) >= 15000:
            connection.executemany(insert_sql, batch)
            batch.clear()

    if batch:
        connection.executemany(insert_sql, batch)

    print(f"Imported {table_name}: {row_count:,} rows")
    return row_count


def create_indexes(connection):
    indexes = {
        "idx_stops_stop_id": ("stops", ["stop_id"]),
        "idx_stops_stop_name": ("stops", ["stop_name"]),
        "idx_routes_route_id": ("routes", ["route_id"]),
        "idx_routes_route_short_name": ("routes", ["route_short_name"]),
        "idx_routes_agency_id": ("routes", ["agency_id"]),
        "idx_trips_trip_id": ("trips", ["trip_id"]),
        "idx_trips_route_id": ("trips", ["route_id"]),
        "idx_trips_service_id": ("trips", ["service_id"]),
        "idx_calendar_dates_service": ("calendar_dates", ["service_id", "date"]),
    }

    for index_name, (table, columns) in indexes.items():
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()

        if not table_exists:
            continue

        existing_columns = {
            row[1]
            for row in connection.execute(f"PRAGMA table_info({quote_identifier(table)})")
        }

        if all(column in existing_columns for column in columns):
            column_sql = ", ".join(quote_identifier(col) for col in columns)
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {quote_identifier(index_name)} "
                f"ON {quote_identifier(table)} ({column_sql})"
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gtfs", required=True, help="Path to GTFS ZIP archive")
    parser.add_argument("--output", required=True, help="Path to output SQLite database")
    args = parser.parse_args()

    gtfs_path = Path(args.gtfs)
    output_path = Path(args.output)

    if not gtfs_path.is_file():
        raise FileNotFoundError(f"GTFS ZIP not found: {gtfs_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    with zipfile.ZipFile(gtfs_path, "r") as archive:
        bad = archive.testzip()
        if bad:
            raise ValueError(f"Corrupted ZIP: {bad}")

        active_service_ids, trip_id_to_int = collect_active_services_and_map_trips(archive)

        connection = sqlite3.connect(output_path)

        try:
            connection.execute("PRAGMA journal_mode=MEMORY")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA page_size=4096")

            connection.execute("""
                CREATE TABLE import_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

            imported_gtfs = set()

            for member in archive.namelist():
                if member.endswith("/"):
                    continue
                suffix = Path(member).suffix.lower()
                if suffix not in (".txt", ".csv"):
                    continue

                table_name = Path(member).stem.lower()
                table_name = re.sub(r"[^a-z0-9_]", "_", table_name)

                if table_name not in ALLOWED_GTFS_TABLES:
                    continue

                data = archive.read(member)
                import_csv(
                    connection=connection,
                    table_name=table_name,
                    data=data,
                    active_service_ids=active_service_ids,
                    trip_id_to_int=trip_id_to_int
                )
                imported_gtfs.add(table_name)

            missing = REQUIRED_GTFS_TABLES - imported_gtfs
            if missing:
                raise ValueError("Missing required GTFS tables: " + ", ".join(sorted(missing)))

            metadata = {
                "created_utc": datetime.now(timezone.utc).isoformat(),
                "gtfs_source": gtfs_path.name,
                "gtfs_tables": ",".join(sorted(imported_gtfs)),
                "format_version": "5",
                "compact_trip_ids": "true",
            }

            connection.executemany(
                "INSERT INTO import_metadata (key, value) VALUES (?, ?)",
                metadata.items(),
            )

            print("Creating core indexes...")
            create_indexes(connection)
            connection.commit()

            print("Compacting database (VACUUM)...")
            connection.execute("VACUUM")

            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise RuntimeError(f"SQLite integrity check failed: {integrity}")

            print("SQLite integrity check: OK")
            print(f"Database created: {output_path}")

            db_size = output_path.stat().st_size
            print(f"Database size: {db_size} bytes")

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
