"""
Database Schema Inspector

Reads a SQLite database file and extracts table names and schemas.
Optionally filter to specific tables by providing table names as arguments.
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
Output:
  When -o/--output is specified:
    Creates directory with tables.json and schemas.json files
  When -o/--output is omitted:
    Writes combined JSON to stdout

Examples:
  # Extract all tables to directory
  pawalogs-db-inspector database.db -o output/

  # Extract specific tables to directory
  pawalogs-db-inspector database.db -o output/ table1 table2

  # Output all tables to stdout
  pawalogs-db-inspector database.db

  # Output specific tables to stdout
  pawalogs-db-inspector database.db table1 table2

  # Works with .EPSQL and .PLSQL files
  pawalogs-db-inspector data/powerlog.PLSQL -o output/
        """,
    )

    parser.add_argument("db_path", help="Path to the SQLite database file")
    parser.add_argument(
        "-o",
        "--output",
        help="Output directory path (if not specified, writes to stdout)",
        default=None,
    )
    parser.add_argument(
        "table_name",
        help="Optional table name(s) to extract schema for (default: all tables)",
        nargs="*",
        default=None,
    )

    args = parser.parse_args()

    db_file = Path(args.db_path)

    if not db_file.exists():
        print(f"Error: Database file not found: {args.db_path}", file=sys.stderr)
        sys.exit(1)

    if not db_file.is_file():
        print(f"Error: Path is not a file: {args.db_path}", file=sys.stderr)
        sys.exit(1)

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        if args.output:
            print(f"Reading database: {db_file.name}", file=sys.stderr)

        if args.table_name:
            table_names = args.table_name
        else:
            table_names = get_table_names(cursor)

        if args.output:
            print(f"Found {len(table_names)} tables", file=sys.stderr)

        schemas = {}
        for table_name in table_names:
            if args.output:
                print(f"  Extracting schema for: {table_name}", file=sys.stderr)
            schema = get_table_schema(cursor, table_name)
            schemas[table_name] = asdict(schema)

        conn.close()

        if args.output:
            output_dir = Path(args.output)
            output_dir.mkdir(parents=True, exist_ok=True)

            tables_output = output_dir / "tables.json"
            with open(tables_output, "w") as f:
                json.dump({"tables": table_names}, f, indent=2)
            print(f"Table names written to: {tables_output}", file=sys.stderr)

            schemas_output = output_dir / "schemas.json"
            with open(schemas_output, "w") as f:
                json.dump(schemas, f, indent=2)
            print(f"Table schemas written to: {schemas_output}", file=sys.stderr)

            print("\nSchema extraction complete!", file=sys.stderr)
        else:
            output = {"tables": table_names, "schemas": schemas}
            json.dump(output, sys.stdout, indent=2)
            print()

    except sqlite3.Error as e:
        print(f"Error reading database: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
