"""
Table Counts

Counts the number of entries in each table of a SQLite database.
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from pawalogs.utils import get_table_names, quote_identifier


def get_table_counts(
    cursor: sqlite3.Cursor, table_names: list[str], min_rows: int = 0
) -> dict[str, int]:
    """
    Get row counts for all tables.

    Args:
        cursor: Database cursor
        table_names: List of table names to count
        min_rows: Minimum number of rows to include in results (filter)

    Returns:
        Dictionary mapping table names to row counts
    """
    counts = {}

    for table_name in table_names:
        quoted_table = quote_identifier(table_name)
        cursor.execute(f"SELECT COUNT(*) FROM {quoted_table}")
        count = cursor.fetchone()[0]

        if count >= min_rows:
            counts[table_name] = count

    return counts


def main():
    """Entry point for the table counts command."""
    parser = argparse.ArgumentParser(
        description="Count entries in each table of a SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Output to stdout
  pawalogs-table-counts database.db

  # Output to file
  pawalogs-table-counts database.db -o counts.json

  # Filter out tables with less than 10 rows
  pawalogs-table-counts database.db --min-rows 10

  # Combine filtering and file output
  pawalogs-table-counts database.db -o counts.json --min-rows 100
        """,
    )

    parser.add_argument("db_path", help="Path to the SQLite database file")
    parser.add_argument(
        "-o",
        "--output",
        help="Output file path (if not specified, writes to stdout)",
        default=None,
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=0,
        help="Minimum number of rows to include table in output (default: 0)",
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

        table_names = get_table_names(cursor)

        counts = get_table_counts(cursor, table_names, min_rows=args.min_rows)

        output_data = {
            "database": db_file.name,
            "total_tables": len(table_names),
            "filtered_tables": len(counts),
            "min_rows_filter": args.min_rows,
            "table_counts": counts,
        }

        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(output_data, f, indent=2)
            print(f"Table counts written to: {output_path}", file=sys.stderr)
        else:
            json.dump(output_data, sys.stdout, indent=2)
            print()

        conn.close()

    except sqlite3.Error as e:
        print(f"Error reading database: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
