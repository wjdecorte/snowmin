"""Stream management operations for Snowflake"""

import re
import click
from typing import Optional
from colorama import Fore, Style
from snowmin.core.connection import ConnectionManager
from snowmin.core.config import get_merged_connection_config


def _parse_schema_spec(schema_spec: Optional[str], config_database: Optional[str]):
    """Parse schema specification which can be 'schema' or 'database.schema'.
    Returns (database, schema) tuple.
    """
    if not schema_spec:
        return config_database, None

    if "." in schema_spec:
        parts = schema_spec.split(".", 1)
        return parts[0], parts[1]
    else:
        return config_database, schema_spec


def _get_mode_color(mode: str) -> str:
    """Get color for stream mode"""
    mode_upper = mode.upper()
    if mode_upper == "DEFAULT":
        return Fore.GREEN
    elif mode_upper == "APPEND_ONLY":
        return Fore.CYAN
    elif mode_upper == "INSERT_ONLY":
        return Fore.BLUE
    else:
        return Fore.WHITE


def _build_schema_query_suffix(target_database, target_schema):
    """Build the IN SCHEMA / IN DATABASE suffix for SHOW queries."""
    if target_schema:
        if target_database:
            return f" IN SCHEMA {target_database}.{target_schema}"
        else:
            raise click.ClickException(
                f"Cannot query schema '{target_schema}' without a database. "
                f"Either set 'database' in your connection config or use --schema DATABASE.SCHEMA format."
            )
    elif target_database:
        return f" IN DATABASE {target_database}"
    return ""


def _location_label(target_database, target_schema):
    """Build a human-readable location label for echo messages."""
    if target_schema:
        return f" from {target_database}.{target_schema}"
    elif target_database:
        return f" from database {target_database}"
    return ""


def _derive_owner_role(database: str, schema: str) -> str:
    """Derive the owner role name from database and schema.

    Database name convention: <prefix>_<env>_<suffix...>
    Owner role pattern: ROLE_<env>_<schema>_OWNER

    Example: database=RAP_SANDBOX_ANALYTICS_DB, schema=SILVER
             -> ROLE_SANDBOX_SILVER_OWNER
    """
    parts = database.upper().split("_", 2)
    if len(parts) < 2:
        raise click.ClickException(
            f"Cannot derive owner role: database '{database}' does not follow "
            f"<prefix>_<env>_... naming convention."
        )
    env = parts[1]
    return f"ROLE_{env}_{schema.upper()}_OWNER"


def _fetch_stream_has_data(conn_config: dict, streams: list) -> dict:
    """Fetch has-data status for a list of streams using SYSTEM$STREAM_HAS_DATA.
    Returns a dict mapping stream_name -> bool (True/False/None on error).

    streams: list of tuples (schema, name, table, mode, stale, type)
    """
    if not streams:
        return {}

    status_map = {}
    batch_size = 50

    for i in range(0, len(streams), batch_size):
        batch = streams[i : i + batch_size]
        queries = []

        for s_schema, s_name, s_table, *_ in batch:
            # Build qualified name from schema info embedded in table name
            # s_table from SHOW STREAMS is already fully qualified
            fqn = f"{s_schema}.{s_name}"
            queries.append(
                f"SELECT '{s_name}' as stream_name, "
                f"SYSTEM$STREAM_HAS_DATA('{fqn}') as has_data"
            )

        full_query = " UNION ALL ".join(queries)

        try:
            cursor = ConnectionManager.execute(full_query, conn_config=conn_config)
            rows = cursor.fetchall()
            cursor.close()

            for row in rows:
                name = row[0]
                has_data = row[1]
                if isinstance(has_data, bool):
                    status_map[name] = has_data
                elif isinstance(has_data, str):
                    status_map[name] = has_data.lower() == "true"
                else:
                    status_map[name] = None

        except Exception as e:
            click.echo(
                f"{Fore.RED}Error fetching stream data status: {e}{Style.RESET_ALL}",
                err=True,
            )

    return status_map


def list_streams_command(
    ctx,
    pattern: Optional[str] = None,
    schema: Optional[str] = None,
    has_data: Optional[bool] = None,
):
    """List streams"""
    settings = ctx.obj["settings"]
    cli_overrides = ctx.obj["cli_overrides"]
    conn_config = get_merged_connection_config(settings, cli_overrides)

    try:
        target_schema_spec = (
            schema or conn_config.get("schema") or conn_config.get("schema_name")
        )
        config_database = conn_config.get("database")

        target_database, target_schema = _parse_schema_spec(
            target_schema_spec, config_database
        )

        query = "SHOW STREAMS" + _build_schema_query_suffix(
            target_database, target_schema
        )

        click.echo(
            f"Fetching streams{_location_label(target_database, target_schema)}..."
        )

        cursor = ConnectionManager.execute(query, conn_config=conn_config)

        # Helper to find column index case-insensitively
        col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
        name_idx = col_map.get("NAME")
        schema_idx = col_map.get("SCHEMA_NAME")
        table_name_idx = col_map.get("TABLE_NAME")
        mode_idx = col_map.get("MODE")
        stale_idx = col_map.get("STALE")
        type_idx = col_map.get("TYPE")

        if name_idx is None:
            click.echo(
                f"{Fore.RED}Error: Could not find 'name' column in SHOW STREAMS result.{Style.RESET_ALL}",
                err=True,
            )
            cursor.close()
            return

        rows = cursor.fetchall()
        cursor.close()

        if not rows:
            click.echo("No streams found.")
            return

        # Apply basic filters first
        filtered = []
        for row in rows:
            s_name = row[name_idx]
            s_schema = row[schema_idx] if schema_idx is not None else "UNKNOWN"
            s_table = row[table_name_idx] if table_name_idx is not None else "UNKNOWN"
            s_mode = row[mode_idx] if mode_idx is not None else "UNKNOWN"
            s_stale = row[stale_idx] if stale_idx is not None else "UNKNOWN"
            s_type = row[type_idx] if type_idx is not None else "UNKNOWN"

            if pattern and not re.search(pattern, s_name):
                continue

            filtered.append((s_schema, s_name, s_table, s_mode, s_stale, s_type))

        if not filtered:
            click.echo("No streams matched the given filters.")
            return

        # Fetch has-data status for each stream
        click.echo(f"Checking data status for {len(filtered)} stream(s)...")
        has_data_map = _fetch_stream_has_data(conn_config, filtered)

        # Apply secondary filter for has_data if specified
        final_filtered = []
        if has_data is not None:
            for stream in filtered:
                s_name = stream[1]
                stream_has_data = has_data_map.get(s_name)
                # Only include streams where we successfully got a boolean, and it matches the filter
                if stream_has_data is has_data:
                    final_filtered.append(stream)
            filtered = final_filtered

            if not filtered:
                click.echo("No streams matched the has-data filter.")
                return

        click.echo(f"\nFound {len(filtered)} stream(s):")
        click.echo("-" * 90)

        for s_schema, s_name, s_table, s_mode, s_stale, s_type in filtered:
            mode_color = _get_mode_color(s_mode)
            stale_color = Fore.RED if str(s_stale).lower() == "true" else Fore.GREEN
            stale_label = "STALE" if str(s_stale).lower() == "true" else "OK"
            stream_has_data = has_data_map.get(s_name)
            if stream_has_data is True:
                data_color = Fore.GREEN
                data_label = "HAS DATA"
            elif stream_has_data is False:
                data_color = Fore.YELLOW
                data_label = "EMPTY"
            else:
                data_color = Fore.WHITE
                data_label = "UNKNOWN"
            click.echo(
                f"{s_schema}.{s_name}: "
                f"{mode_color}{s_mode}{Style.RESET_ALL} | "
                f"on {Fore.CYAN}{s_table}{Style.RESET_ALL} | "
                f"{stale_color}{stale_label}{Style.RESET_ALL} | "
                f"{data_color}{data_label}{Style.RESET_ALL}"
            )

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)


def create_stream_command(
    ctx,
    stream_name: str,
    source_table: str,
    schema: Optional[str] = None,
    mode: Optional[str] = None,
    before: Optional[str] = None,
    at: Optional[str] = None,
    comment: Optional[str] = None,
):
    """Create a stream on a table"""
    settings = ctx.obj["settings"]
    cli_overrides = ctx.obj["cli_overrides"]
    conn_config = get_merged_connection_config(settings, cli_overrides)

    try:
        target_schema_spec = (
            schema or conn_config.get("schema") or conn_config.get("schema_name")
        )
        config_database = conn_config.get("database")

        target_database, target_schema = _parse_schema_spec(
            target_schema_spec, config_database
        )

        # Build fully qualified stream name
        if target_database and target_schema:
            full_stream_name = f"{target_database}.{target_schema}.{stream_name}"
        elif target_schema:
            full_stream_name = f"{target_schema}.{stream_name}"
        else:
            full_stream_name = stream_name

        # Build fully qualified source table name (if not already qualified)
        if "." not in source_table:
            if target_database and target_schema:
                full_source_table = f"{target_database}.{target_schema}.{source_table}"
            elif target_schema:
                full_source_table = f"{target_schema}.{source_table}"
            else:
                full_source_table = source_table
        else:
            full_source_table = source_table

        query = f"CREATE STREAM {full_stream_name} ON TABLE {full_source_table}"

        # Add mode clause
        if mode:
            mode_upper = mode.upper()
            if mode_upper == "APPEND_ONLY":
                query += " APPEND_ONLY = TRUE"
            elif mode_upper == "INSERT_ONLY":
                query += " INSERT_ONLY = TRUE"
            elif mode_upper != "DEFAULT":
                raise click.ClickException(
                    f"Unknown stream mode: {mode}. Use DEFAULT, APPEND_ONLY, or INSERT_ONLY."
                )

        # Add point-in-time clause
        if at:
            query += f" AT (TIMESTAMP => '{at}'::TIMESTAMP_LTZ)"
        elif before:
            query += f" BEFORE (TIMESTAMP => '{before}'::TIMESTAMP_LTZ)"

        # Add comment (use default if none provided)
        if not comment and target_schema and source_table:
            # Extract bare table name (strip any db.schema prefix)
            bare_table = (
                source_table.rsplit(".", 1)[-1] if "." in source_table else source_table
            )
            comment = f"Stream for tracking changes in {target_database}.{target_schema}.{bare_table} table"
        if comment:
            escaped = comment.replace("'", "''")
            query += f" COMMENT = '{escaped}'"

        # Switch to owner role before creating
        if target_database and target_schema:
            owner_role = _derive_owner_role(target_database, target_schema)
            use_role_query = f"USE ROLE {owner_role}"
            click.echo(f"Executing: {use_role_query}")
            cursor = ConnectionManager.execute(use_role_query, conn_config=conn_config)
            cursor.close()

        click.echo(f"Executing: {query}")
        cursor = ConnectionManager.execute(query, conn_config=conn_config)
        cursor.close()
        click.echo(
            f"{Fore.GREEN}Successfully created stream: {full_stream_name}{Style.RESET_ALL}"
        )

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)


def drop_stream_command(
    ctx,
    stream_name: str,
    schema: Optional[str] = None,
):
    """Drop a stream"""
    settings = ctx.obj["settings"]
    cli_overrides = ctx.obj["cli_overrides"]
    conn_config = get_merged_connection_config(settings, cli_overrides)

    try:
        target_schema_spec = (
            schema or conn_config.get("schema") or conn_config.get("schema_name")
        )
        config_database = conn_config.get("database")

        target_database, target_schema = _parse_schema_spec(
            target_schema_spec, config_database
        )

        # Build fully qualified stream name
        if target_database and target_schema:
            full_stream_name = f"{target_database}.{target_schema}.{stream_name}"
        elif target_schema:
            full_stream_name = f"{target_schema}.{stream_name}"
        else:
            full_stream_name = stream_name

        if not click.confirm(
            f"Are you sure you want to drop stream '{full_stream_name}'?"
        ):
            click.echo("Operation cancelled.")
            return

        query = f"DROP STREAM {full_stream_name}"
        click.echo(f"Executing: {query}")
        cursor = ConnectionManager.execute(query, conn_config=conn_config)
        cursor.close()
        click.echo(
            f"{Fore.GREEN}Successfully dropped stream: {full_stream_name}{Style.RESET_ALL}"
        )

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)


def reset_stream_command(
    ctx,
    stream_name: str,
    schema: Optional[str] = None,
    at: Optional[str] = None,
):
    """Reset a stream by dropping and recreating it, optionally at a point in time.

    Fetches the stream's DDL via GET_DDL, drops it, then recreates it.
    If --at is provided, appends AT (TIMESTAMP => ...) to the CREATE statement.
    """
    settings = ctx.obj["settings"]
    cli_overrides = ctx.obj["cli_overrides"]
    conn_config = get_merged_connection_config(settings, cli_overrides)

    try:
        target_schema_spec = (
            schema or conn_config.get("schema") or conn_config.get("schema_name")
        )
        config_database = conn_config.get("database")

        target_database, target_schema = _parse_schema_spec(
            target_schema_spec, config_database
        )

        # Build fully qualified stream name
        if target_database and target_schema:
            full_stream_name = f"{target_database}.{target_schema}.{stream_name}"
        elif target_schema:
            full_stream_name = f"{target_schema}.{stream_name}"
        else:
            full_stream_name = stream_name

        # 1. Fetch DDL
        click.echo(f"Fetching DDL for stream {full_stream_name}...")
        ddl_query = f"SELECT GET_DDL('stream', '{full_stream_name}')"
        cursor = ConnectionManager.execute(ddl_query, conn_config=conn_config)
        res = cursor.fetchone()
        cursor.close()

        if not res or not res[0]:
            raise click.ClickException(
                f"Could not fetch DDL for stream {full_stream_name}"
            )
        ddl = res[0].strip().rstrip(";")

        click.echo(f"Current DDL:\n{Fore.CYAN}{ddl}{Style.RESET_ALL}\n")

        # 2. Extract existing comment before dropping
        existing_comment = None
        show_query = f"SHOW STREAMS LIKE '{stream_name}'"
        show_query += _build_schema_query_suffix(target_database, target_schema)
        cursor = ConnectionManager.execute(show_query, conn_config=conn_config)
        col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
        comment_idx = col_map.get("COMMENT")
        show_rows = cursor.fetchall()
        cursor.close()

        if show_rows and comment_idx is not None:
            existing_comment = show_rows[0][comment_idx]
            if existing_comment:
                click.echo(
                    f"Preserving comment: {Fore.YELLOW}{existing_comment}{Style.RESET_ALL}"
                )

        # If point-in-time requested, append AT clause to DDL
        if at:
            ddl += f" AT (TIMESTAMP => '{at}'::TIMESTAMP_LTZ)"
            click.echo(
                f"Will recreate with point-in-time: {Fore.YELLOW}{at}{Style.RESET_ALL}"
            )

        if not click.confirm(
            f"Are you sure you want to reset stream '{full_stream_name}'?"
        ):
            click.echo("Operation cancelled.")
            return

        # Switch to owner role before drop/recreate
        if target_database and target_schema:
            owner_role = _derive_owner_role(target_database, target_schema)
            use_role_query = f"USE ROLE {owner_role}"
            click.echo(f"Executing: {use_role_query}")
            cursor = ConnectionManager.execute(use_role_query, conn_config=conn_config)
            cursor.close()

        # 3. Drop stream
        drop_query = f"DROP STREAM {full_stream_name}"
        click.echo(f"Executing: {drop_query}")
        cursor = ConnectionManager.execute(drop_query, conn_config=conn_config)
        cursor.close()

        # 4. Recreate stream (set schema context so DDL resolves correctly)
        if target_database and target_schema:
            use_query = f"USE SCHEMA {target_database}.{target_schema}"
            click.echo(f"Setting context: {use_query}")
            cursor = ConnectionManager.execute(use_query, conn_config=conn_config)
            cursor.close()

        click.echo(f"Recreating stream {stream_name}...")
        cursor = ConnectionManager.execute(ddl, conn_config=conn_config)
        cursor.close()
        click.echo(
            f"{Fore.GREEN}Successfully reset stream: {full_stream_name}{Style.RESET_ALL}"
        )

        # 5. Restore existing comment if present
        if existing_comment:
            escaped = existing_comment.replace("'", "''")
            comment_query = f"ALTER STREAM {full_stream_name} SET COMMENT = '{escaped}'"
            click.echo(f"Executing: {comment_query}")
            cursor = ConnectionManager.execute(comment_query, conn_config=conn_config)
            cursor.close()
            click.echo(f"{Fore.GREEN}Restored comment on stream{Style.RESET_ALL}")

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)
