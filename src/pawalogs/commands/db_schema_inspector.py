"""
Database Schema Inspector

Reads a SQLite database file and extracts table names and schemas.
"""

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict
from pathlib import Path

from pawalogs.utils import (
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    TableSchema,
    get_table_names,
    quote_identifier,
)


def get_table_schema(cursor: sqlite3.Cursor, table_name: str) -> TableSchema:
    """Get the schema for a specific table."""
    quoted_table = quote_identifier(table_name)

    cursor.execute(f"PRAGMA table_info({quoted_table})")
    columns_data = cursor.fetchall()

    columns = [
        ColumnInfo(
            cid=col[0],
            name=col[1],
            type=col[2],
            not_null=bool(col[3]),
            default_value=col[4],
            primary_key=bool(col[5]),
        )
        for col in columns_data
    ]

    cursor.execute(f"PRAGMA foreign_key_list({quoted_table})")
    foreign_keys_data = cursor.fetchall()

    foreign_keys = [
        ForeignKeyInfo(
            id=fk[0],
            seq=fk[1],
            table=fk[2],
            from_column=fk[3],
            to_column=fk[4],
            on_update=fk[5],
            on_delete=fk[6],
            match=fk[7],
        )
        for fk in foreign_keys_data
    ]

    cursor.execute(f"PRAGMA index_list({quoted_table})")
    indexes_data = cursor.fetchall()

    indexes = [
        IndexInfo(
            name=idx[1],
            unique=bool(idx[2]),
            origin=idx[3],
            partial=bool(idx[4]),
        )
        for idx in indexes_data
    ]

    return TableSchema(
        table_name=table_name,
        columns=columns,
        foreign_keys=foreign_keys,
        indexes=indexes,
    )


def main():
    """Entry point for the database schema inspector."""
    parser = argparse.ArgumentParser(
        description="Extract table names and schemas from a SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Output files:
  tables.json  - List of all table names
  schemas.json - Complete schema for each table (columns, types, constraints,
                 foreign keys, indexes)

Examples:
  # Extract schema from database
  pawalogs-db-inspector database.db output/

  # Works with .EPSQL and .PLSQL files
  pawalogs-db-inspector data/powerlog.PLSQL output/
        """,
    )

    parser.add_argument("db_path", help="Path to the SQLite database file")
    parser.add_argument(
        "output_path",
        help="Directory where output JSON files will be written",
    )

    args = parser.parse_args()

    db_file = Path(args.db_path)
    output_dir = Path(args.output_path)

    if not db_file.exists():
        print(f"Error: Database file not found: {args.db_path}", file=sys.stderr)
        sys.exit(1)

    if not db_file.is_file():
        print(f"Error: Path is not a file: {args.db_path}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        print(f"Reading database: {db_file.name}")
        table_names = get_table_names(cursor)
        print(f"Found {len(table_names)} tables")

        tables_output = output_dir / "tables.json"
        with open(tables_output, "w") as f:
            json.dump({"tables": table_names}, f, indent=2)
        print(f"Table names written to: {tables_output}")

        schemas = {}
        for table_name in table_names:
            print(f"  Extracting schema for: {table_name}")
            schema = get_table_schema(cursor, table_name)
            schemas[table_name] = asdict(schema)

        schemas_output = output_dir / "schemas.json"
        with open(schemas_output, "w") as f:
            json.dump(schemas, f, indent=2)
        print(f"Table schemas written to: {schemas_output}")

        conn.close()
        print("\nSchema extraction complete!")

    except sqlite3.Error as e:
        print(f"Error reading database: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
