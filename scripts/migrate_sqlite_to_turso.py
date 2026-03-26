import argparse
import os
import sqlite3
from pathlib import Path

try:
    import libsql
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'libsql'. Install requirements before running migration."
    ) from exc


BATCH_SIZE = 500


def parse_args():
    parser = argparse.ArgumentParser(
        description="Migrate a local SQLite database into a Turso database."
    )
    parser.add_argument(
        "--source",
        default="data/nepthanh.db",
        help="Path to the source SQLite database file.",
    )
    parser.add_argument(
        "--replica-path",
        default=".turso-migrate-replica.db",
        help="Local replica path used by libsql during migration.",
    )
    parser.add_argument(
        "--url",
        default=(os.environ.get("TURSO_DATABASE_URL") or "").strip(),
        help="Turso database URL. Defaults to TURSO_DATABASE_URL.",
    )
    parser.add_argument(
        "--token",
        default=(os.environ.get("TURSO_AUTH_TOKEN") or "").strip(),
        help="Turso auth token. Defaults to TURSO_AUTH_TOKEN.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop existing user tables on the target before importing.",
    )
    return parser.parse_args()


def quote_ident(name):
    return '"' + str(name).replace('"', '""') + '"'


def connect_source(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def connect_target(replica_path, url, token):
    conn = libsql.connect(replica_path, sync_url=url, auth_token=token)
    conn.sync()
    return conn


def fetch_objects(conn, object_type):
    rows = conn.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = ?
          AND name NOT LIKE 'sqlite_%'
          AND sql IS NOT NULL
        ORDER BY name
        """,
        (object_type,),
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def fetch_table_names(conn):
    return [name for name, _ in fetch_objects(conn, "table")]


def target_has_user_tables(conn):
    return bool(fetch_table_names(conn))


def drop_existing_target_objects(conn):
    for object_type in ("view", "trigger"):
        for name, _ in fetch_objects(conn, object_type):
            conn.execute(f"DROP {object_type.upper()} IF EXISTS {quote_ident(name)}")

    for table_name in fetch_table_names(conn):
        conn.execute(f"DROP TABLE IF EXISTS {quote_ident(table_name)}")


def create_schema(source_conn, target_conn):
    for _, sql in fetch_objects(source_conn, "table"):
        target_conn.execute(sql)

    for _, sql in fetch_objects(source_conn, "index"):
        target_conn.execute(sql)

    for _, sql in fetch_objects(source_conn, "trigger"):
        target_conn.execute(sql)

    for _, sql in fetch_objects(source_conn, "view"):
        target_conn.execute(sql)


def copy_table_data(source_conn, target_conn, table_name):
    column_rows = source_conn.execute(
        f"PRAGMA table_info({quote_ident(table_name)})"
    ).fetchall()
    columns = [row["name"] for row in column_rows]
    if not columns:
        return 0

    column_list = ", ".join(quote_ident(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = (
        f"INSERT INTO {quote_ident(table_name)} ({column_list}) VALUES ({placeholders})"
    )

    row_count = 0
    batch = []
    rows = source_conn.execute(f"SELECT * FROM {quote_ident(table_name)}").fetchall()
    for row in rows:
        batch.append(tuple(row[column] for column in columns))
        if len(batch) >= BATCH_SIZE:
            for params in batch:
                target_conn.execute(insert_sql, params)
            target_conn.commit()
            row_count += len(batch)
            batch = []

    if batch:
        for params in batch:
            target_conn.execute(insert_sql, params)
        target_conn.commit()
        row_count += len(batch)

    return row_count


def copy_sqlite_sequence(source_conn, target_conn):
    sequence_exists = source_conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name = 'sqlite_sequence'"
    ).fetchone()
    if not sequence_exists:
        return

    rows = source_conn.execute("SELECT name, seq FROM sqlite_sequence").fetchall()
    if not rows:
        return

    for row in rows:
        table_name = row["name"]
        seq = row["seq"]
        target_conn.execute(
            "DELETE FROM sqlite_sequence WHERE name = ?",
            (table_name,),
        )
        target_conn.execute(
            "INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)",
            (table_name, seq),
        )
    target_conn.commit()


def migrate(source_path, replica_path, url, token, reset):
    if not source_path.exists():
        raise SystemExit(f"Source database not found: {source_path}")
    if not url or not token:
        raise SystemExit(
            "Missing Turso credentials. Provide --url/--token or set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN."
        )

    source_conn = connect_source(str(source_path))
    target_conn = connect_target(str(replica_path), url, token)

    try:
        target_conn.execute("PRAGMA foreign_keys = OFF")
        target_conn.commit()

        if target_has_user_tables(target_conn):
            if not reset:
                raise SystemExit(
                    "Target Turso database already has user tables. Re-run with --reset to replace it."
                )
            drop_existing_target_objects(target_conn)
            target_conn.commit()

        create_schema(source_conn, target_conn)
        target_conn.commit()

        migrated = []
        for table_name in fetch_table_names(source_conn):
            row_count = copy_table_data(source_conn, target_conn, table_name)
            migrated.append((table_name, row_count))

        copy_sqlite_sequence(source_conn, target_conn)
        target_conn.execute("PRAGMA foreign_keys = ON")
        target_conn.commit()
        target_conn.sync()
    finally:
        source_conn.close()
        target_conn.close()

    print("Migration completed.")
    for table_name, row_count in migrated:
        print(f"- {table_name}: {row_count} rows")


def main():
    args = parse_args()
    source_path = Path(args.source).resolve()
    replica_path = Path(args.replica_path).resolve()
    replica_path.parent.mkdir(parents=True, exist_ok=True)
    migrate(
        source_path=source_path,
        replica_path=replica_path,
        url=args.url,
        token=args.token,
        reset=args.reset,
    )


if __name__ == "__main__":
    main()
