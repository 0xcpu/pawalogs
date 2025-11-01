"""Shared utilities for pawalogs scripts."""

import sqlite3
from dataclasses import dataclass, field
from typing import Any


def get_version():
    """Return the current version."""
    from pawalogs import __version__

    return __version__


def quote_identifier(identifier: str) -> str:
    """
    Safely quote a SQLite identifier (table/column name).

    This function escapes double quotes in the identifier and wraps it in
    double quotes to protect against SQL injection and handle special characters.

    Args:
        identifier: The table or column name to quote

    Returns:
        Safely quoted identifier

    Examples:
        >>> quote_identifier("table_name")
        '"table_name"'
        >>> quote_identifier('table-with-hyphen')
        '"table-with-hyphen"'
        >>> quote_identifier('table"with"quotes')
        '"table""with""quotes"'
    """
    # Escape any double quotes in the identifier by doubling them
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def get_table_names(cursor: sqlite3.Cursor) -> list[str]:
    """
    Get all table names from a SQLite database.

    Filters out sqlite internal tables (those starting with 'sqlite_').

    Args:
        cursor: SQLite database cursor

    Returns:
        List of table names, sorted alphabetically
    """
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    # Filter out sqlite internal tables
    return [t for t in tables if not t.startswith("sqlite_")]


@dataclass
class ColumnInfo:
    """Schema information for a database column."""

    cid: int
    name: str
    type: str
    not_null: bool
    default_value: Any
    primary_key: bool


@dataclass
class ForeignKeyInfo:
    """Foreign key constraint information."""

    id: int
    seq: int
    table: str
    from_column: str
    to_column: str
    on_update: str
    on_delete: str
    match: str


@dataclass
class IndexInfo:
    """Index information for a database table."""

    name: str
    unique: bool
    origin: str
    partial: bool


@dataclass
class TableSchema:
    """Complete schema information for a database table."""

    table_name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
