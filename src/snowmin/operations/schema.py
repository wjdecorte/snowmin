"""Helpers for command schema selection."""

from __future__ import annotations

from typing import Optional

import click


def parse_schema_spec(schema_spec: Optional[str], config_database: Optional[str]):
    """Parse 'schema' or 'database.schema' into a (database, schema) tuple."""
    if not schema_spec:
        return config_database, None

    if "." in schema_spec:
        database, schema = schema_spec.split(".", 1)
        return database, schema

    return config_database, schema_spec


def parse_schema_specs(schema_spec: Optional[str], config_database: Optional[str]):
    """Parse an optional comma-separated schema list."""
    if not schema_spec:
        return [(config_database, None)]

    specs = [spec.strip() for spec in schema_spec.split(",") if spec.strip()]
    if not specs:
        return [(config_database, None)]

    return [parse_schema_spec(spec, config_database) for spec in specs]


def build_schema_query_suffix(target_database, target_schema):
    """Build the IN SCHEMA / IN DATABASE suffix for SHOW queries."""
    if target_schema:
        if target_database:
            return f" IN SCHEMA {target_database}.{target_schema}"
        raise click.ClickException(
            f"Cannot query schema '{target_schema}' without a database. "
            f"Either set 'database' in your connection config or use --schema DATABASE.SCHEMA format."
        )

    if target_database:
        return f" IN DATABASE {target_database}"
    return ""


def location_label(target_database, target_schema):
    """Build a human-readable location label for echo messages."""
    if target_schema:
        return f" from {target_database}.{target_schema}"
    if target_database:
        return f" from database {target_database}"
    return ""
