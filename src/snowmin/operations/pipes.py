"""Pipe management operations for Snowflake"""

import re
import json
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


def _get_status_color(status: str) -> str:
    """Get color for pipe status"""
    status_upper = status.upper()
    if status_upper == "RUNNING":
        return Fore.GREEN
    elif status_upper == "PAUSED":
        return Fore.YELLOW
    elif "STOPPED" in status_upper or "STALLED" in status_upper:
        return Fore.RED
    else:
        return Fore.WHITE


def _fetch_pipe_statuses(conn_config: dict, pipes: list) -> dict:
    """
    Fetch detailed status for a list of pipes using SYSTEM$PIPE_STATUS.
    Returns a dict mapping pipe_name -> execution_state.

    pipes: List of (name, database, schema) tuples
    """
    if not pipes:
        return {}

    status_map = {}
    batch_size = 50  # Process in batches to avoid query size limits

    for i in range(0, len(pipes), batch_size):
        batch = pipes[i : i + batch_size]
        queries = []

        for name, db, schema in batch:
            # Construct fully qualified name
            fqn = name
            if db and schema:
                fqn = f"{db}.{schema}.{name}"
            elif schema:
                fqn = f"{schema}.{name}"

            queries.append(
                f"SELECT '{name}' as pipe_name, SYSTEM$PIPE_STATUS('{fqn}') as json_status"
            )

        full_query = " UNION ALL ".join(queries)

        try:
            cursor = ConnectionManager.execute(full_query, conn_config=conn_config)
            rows = cursor.fetchall()
            cursor.close()

            for row in rows:
                p_name = row[0]
                json_status = row[1]
                try:
                    status_data = json.loads(json_status)
                    execution_state = status_data.get("executionState", "UNKNOWN")
                    status_map[p_name] = execution_state
                except json.JSONDecodeError:
                    status_map[p_name] = "ERROR_PARSING_JSON"

        except Exception as e:
            click.echo(
                f"{Fore.RED}Error fetching status batch: {e}{Style.RESET_ALL}", err=True
            )

    return status_map


def list_pipes_command(
    ctx,
    pattern: Optional[str] = None,
    schema: Optional[str] = None,
    status: Optional[str] = None,
):
    """List pipes"""
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

        query = "SHOW PIPES"
        if target_schema:
            if target_database:
                query += f" IN SCHEMA {target_database}.{target_schema}"
            else:
                raise click.ClickException(
                    f"Cannot query schema '{target_schema}' without a database. "
                    f"Either set 'database' in your connection config or use --schema DATABASE.SCHEMA format."
                )
        elif target_database:
            # Only database specified, no schema
            query += f" IN DATABASE {target_database}"

        click.echo(
            f"Fetching pipes{f' from {target_database}.{target_schema}' if target_schema else f' from database {target_database}' if target_database else ''}..."
        )

        cursor = ConnectionManager.execute(query, conn_config=conn_config)

        # Helper to find column index case-insensitively
        col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
        name_idx = col_map.get("NAME")
        database_idx = col_map.get("DATABASE_NAME")
        schema_idx = col_map.get("SCHEMA_NAME")

        if name_idx is None:
            click.echo(
                f"{Fore.RED}Error: Could not find 'name' column in SHOW PIPES result.{Style.RESET_ALL}",
                err=True,
            )
            cursor.close()
            return

        rows = cursor.fetchall()
        cursor.close()

        if not rows:
            click.echo("No pipes found.")
            return

        # Pre-filter pipes by pattern to minimize SYSTEM$PIPE_STATUS calls
        pipes_to_check = []
        for row in rows:
            p_name = row[name_idx]
            p_database = row[database_idx] if database_idx is not None else None
            p_schema = row[schema_idx] if schema_idx is not None else None

            if pattern and not re.search(pattern, p_name):
                continue

            pipes_to_check.append((p_name, p_database, p_schema))

        if not pipes_to_check:
            click.echo("No pipes found matching pattern.")
            return

        click.echo(f"Fetching detailed status for {len(pipes_to_check)} pipe(s)...")
        status_map = _fetch_pipe_statuses(conn_config, pipes_to_check)

        # Filter by status and display
        filtered_count = 0
        click.echo("-" * 60)

        for p_name, p_database, p_schema in pipes_to_check:
            p_state = status_map.get(p_name, "UNKNOWN")

            # Apply status filter (case-insensitive)
            if status and p_state.upper() != status.upper():
                continue

            filtered_count += 1
            state_color = _get_status_color(p_state)

            display_name = f"{p_schema}.{p_name}"
            # Ensure proper padding for alignment could be added here, but simple format for now:
            click.echo(f"{display_name}: {state_color}{p_state}{Style.RESET_ALL}")

        if filtered_count == 0 and status:
            click.echo(f"No pipes found matching status '{status}'.")
        else:
            click.echo(f"\nTotal displayed: {filtered_count}")

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)


def _process_pipes(
    ctx,
    action: str,
    pipe_name: Optional[str],
    pattern: Optional[str],
    schema: Optional[str],
    status: Optional[str],
):
    """Helper to process pipe actions"""
    if not pipe_name and not pattern:
        raise click.UsageError("Must provide PIPE_NAME or --pattern")

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

        pipes_to_process = []

        if pipe_name:
            # Single pipe specified
            # For single pipe, we might need to fetch status if filtering by status is requested
            # or simply to display it.
            # However, p_state isn't known until we query.
            # If plain pipe_name is given, we assume user wants to act on it regardless of current state
            # UNLESS status filter is provided.

            # Parse possible schema in name
            p_n = pipe_name
            p_d = None
            p_s = None
            if "." in p_n:
                parts = p_n.split(".")
                if len(parts) == 3:
                    p_d, p_s, p_n = parts
                elif len(parts) == 2:
                    p_s, p_n = parts

            # If status filter is applied, we MUST fetch status first.
            current_state = "UNKNOWN"
            if status:
                status_map = _fetch_pipe_statuses(
                    conn_config, [(p_n, p_d or target_database, p_s or target_schema)]
                )
                current_state = status_map.get(p_n, "UNKNOWN")

                if current_state.upper() != status.upper():
                    click.echo(
                        f"Pipe {pipe_name} has status {current_state}, skipping (requested {status})"
                    )
                    return

            pipes_to_process.append((p_n, current_state, p_d, p_s))

        elif pattern:
            # Pattern-based filtering
            query = "SHOW PIPES"
            if target_schema:
                if target_database:
                    query += f" IN SCHEMA {target_database}.{target_schema}"
                else:
                    raise click.ClickException(
                        f"Cannot query schema '{target_schema}' without a database. "
                        f"Either set 'database' in your connection config or use --schema DATABASE.SCHEMA format."
                    )
            elif target_database:
                # Only database specified, no schema
                query += f" IN DATABASE {target_database}"

            click.echo(
                f"Fetching pipes{f' from {target_database}.{target_schema}' if target_schema else f' from database {target_database}' if target_database else ''}..."
            )
            cursor = ConnectionManager.execute(query, conn_config=conn_config)

            # Helper to find column index case-insensitively
            col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
            name_idx = col_map.get("NAME")
            database_idx = col_map.get("DATABASE_NAME")
            schema_idx = col_map.get("SCHEMA_NAME")

            if name_idx is None:
                click.echo(
                    f"{Fore.RED}Error: Could not find 'name' column in SHOW PIPES result.{Style.RESET_ALL}",
                    err=True,
                )
                cursor.close()
                return

            rows = cursor.fetchall()
            cursor.close()

            # First filter by pattern
            candidates = []
            for row in rows:
                p_name = row[name_idx]
                p_database = row[database_idx] if database_idx is not None else None
                p_schema = row[schema_idx] if schema_idx is not None else None

                if not re.search(pattern, p_name):
                    continue
                candidates.append((p_name, p_database, p_schema))

            if not candidates:
                click.echo("No pipes found matching pattern.")
                return

            # Now fetch statuses for candidates
            click.echo(f"Fetching statuses for {len(candidates)} pipes...")
            status_map = _fetch_pipe_statuses(conn_config, candidates)

            for p_name, p_database, p_schema in candidates:
                p_state = status_map.get(p_name, "UNKNOWN")

                # Apply status filter (case-insensitive)
                if status and p_state.upper() != status.upper():
                    continue

                pipes_to_process.append((p_name, p_state, p_database, p_schema))

        if not pipes_to_process:
            click.echo("No pipes found to process.")
            return

        # Display pipes before confirmation (for pattern-based operations)
        if pattern:
            click.echo(f"\nFound {len(pipes_to_process)} pipe(s) to {action}:")
            for pipe_info in pipes_to_process:
                p_name = pipe_info[0] if isinstance(pipe_info, tuple) else pipe_info
                p_state = (
                    pipe_info[1]
                    if isinstance(pipe_info, tuple) and len(pipe_info) > 1
                    else "UNKNOWN"
                )
                state_color = _get_status_color(p_state)
                click.echo(
                    f"  - {Fore.CYAN}{p_name}{Style.RESET_ALL} (Status: {state_color}{p_state}{Style.RESET_ALL})"
                )

            if not click.confirm(
                f"\nAre you sure you want to {action.lower()} {len(pipes_to_process)} pipe(s)?"
            ):
                click.echo("Operation cancelled.")
                return

        for pipe_info in pipes_to_process:
            # Handle both tuple (name, state, database, schema) and string (name only) formats
            if isinstance(pipe_info, tuple):
                p_name = pipe_info[0]
                p_database = pipe_info[2] if len(pipe_info) > 2 else None
                p_schema = pipe_info[3] if len(pipe_info) > 3 else None
            else:
                p_name = pipe_info
                p_database = None
                p_schema = None

            try:
                # Build fully qualified pipe name
                pipe_schema = p_schema or target_schema
                pipe_database = p_database or target_database

                if pipe_database and pipe_schema:
                    full_pipe_name = f"{pipe_database}.{pipe_schema}.{p_name}"
                elif pipe_schema:
                    full_pipe_name = f"{pipe_schema}.{p_name}"
                else:
                    full_pipe_name = p_name

                # Build action-specific SQL
                if action == "REFRESH":
                    query = f"ALTER PIPE {full_pipe_name} REFRESH"
                elif action == "PAUSE":
                    query = (
                        f"ALTER PIPE {full_pipe_name} SET PIPE_EXECUTION_PAUSED = TRUE"
                    )
                elif action == "RESUME":
                    query = (
                        f"ALTER PIPE {full_pipe_name} SET PIPE_EXECUTION_PAUSED = FALSE"
                    )
                else:
                    raise ValueError(f"Unknown action: {action}")

                click.echo(f"Executing: {query}")
                cursor = ConnectionManager.execute(query, conn_config=conn_config)
                cursor.close()
                click.echo(
                    f"{Fore.GREEN}Successfully {action.lower()}ed pipe: {p_name}{Style.RESET_ALL}"
                )
            except Exception as e:
                click.echo(
                    f"{Fore.RED}Error processing pipe {p_name}: {e}{Style.RESET_ALL}",
                    err=True,
                )

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)


def refresh_pipe_command(
    ctx,
    pipe_name: Optional[str],
    pattern: Optional[str],
    schema: Optional[str],
    status: Optional[str],
):
    """Refresh a pipe or multiple pipes"""
    _process_pipes(ctx, "REFRESH", pipe_name, pattern, schema, status)


def pause_pipe_command(
    ctx,
    pipe_name: Optional[str],
    pattern: Optional[str],
    schema: Optional[str],
    status: Optional[str],
):
    """Pause a pipe or multiple pipes"""
    _process_pipes(ctx, "PAUSE", pipe_name, pattern, schema, status)


def resume_pipe_command(
    ctx,
    pipe_name: Optional[str],
    pattern: Optional[str],
    schema: Optional[str],
    status: Optional[str],
):
    """Resume a pipe or multiple pipes"""
    _process_pipes(ctx, "RESUME", pipe_name, pattern, schema, status)


def drop_recreate_pipe_command(
    ctx,
    pipe_name: Optional[str],
    all_flag: bool,
    pattern: Optional[str],
    schema: Optional[str],
    status: Optional[str],
    skip_status: bool = False,
):
    """Drop and recreate one or more pipes using their current DDL"""
    if not pipe_name and not pattern and not all_flag:
        raise click.UsageError("Must provide PIPE_NAME, --pattern, or --all")

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

        pipes_to_process = []

        if pipe_name:
            # Single pipe specified

            # Parse possible schema in name
            p_n = pipe_name
            p_d = None
            p_s = None
            if "." in p_n:
                parts = p_n.split(".")
                if len(parts) == 3:
                    p_d, p_s, p_n = parts
                elif len(parts) == 2:
                    p_s, p_n = parts

            current_state = "UNKNOWN"
            if status and not skip_status:
                status_map = _fetch_pipe_statuses(
                    conn_config, [(p_n, p_d or target_database, p_s or target_schema)]
                )
                current_state = status_map.get(p_n, "UNKNOWN")

                if current_state.upper() != status.upper():
                    click.echo(
                        f"Pipe {pipe_name} has status {current_state}, skipping (requested {status})"
                    )
                    return
            elif not skip_status:
                # Fetch status just for display if not skipping
                status_map = _fetch_pipe_statuses(
                    conn_config, [(p_n, p_d or target_database, p_s or target_schema)]
                )
                current_state = status_map.get(p_n, "UNKNOWN")

            pipes_to_process.append((p_n, current_state, p_d, p_s))

        elif pattern or all_flag:
            # Pattern-based or all pipes
            query = "SHOW PIPES"
            if target_schema:
                if target_database:
                    query += f" IN SCHEMA {target_database}.{target_schema}"
                else:
                    raise click.ClickException(
                        f"Cannot query schema '{target_schema}' without a database. "
                        f"Either set 'database' in your connection config or use --schema DATABASE.SCHEMA format."
                    )
            elif target_database:
                # Only database specified, no schema
                query += f" IN DATABASE {target_database}"

            click.echo(
                f"Fetching pipes{f' from {target_database}.{target_schema}' if target_schema else f' from database {target_database}' if target_database else ''}..."
            )
            cursor = ConnectionManager.execute(query, conn_config=conn_config)

            col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
            name_idx = col_map.get("NAME")
            database_idx = col_map.get("DATABASE_NAME")
            schema_idx = col_map.get("SCHEMA_NAME")

            if name_idx is None:
                click.echo(
                    f"{Fore.RED}Error: Could not find 'name' column in SHOW PIPES result.{Style.RESET_ALL}",
                    err=True,
                )
                cursor.close()
                return

            rows = cursor.fetchall()
            cursor.close()

            # First match filtering
            candidates = []
            for row in rows:
                p_name = row[name_idx]
                p_database = row[database_idx] if database_idx is not None else None
                p_schema = row[schema_idx] if schema_idx is not None else None

                # Apply pattern filter if provided
                if pattern and not re.search(pattern, p_name):
                    continue

                candidates.append((p_name, p_database, p_schema))

            if not candidates:
                click.echo("No pipes found matching pattern.")
                return

            # Fetch detailed statuses unless skipped
            status_map = {}
            if not skip_status:
                click.echo(f"Fetching statuses for {len(candidates)} pipes...")
                status_map = _fetch_pipe_statuses(conn_config, candidates)

            for p_name, p_database, p_schema in candidates:
                p_state = status_map.get(p_name, "UNKNOWN")

                # Apply status filter (case-insensitive)
                # Note: if skip_status is True, p_state is UNKNOWN.
                # If user filters by status AND uses --skip-status, effectively no pipes will match
                # unless they filter for UNKNOWN (which is unlikely what they want).
                # We should probably warn or error if both are used, but for now we follow logic.
                if status and p_state.upper() != status.upper():
                    continue

                pipes_to_process.append((p_name, p_state, p_database, p_schema))

        if not pipes_to_process:
            click.echo("No pipes found to process.")
            return

        # Display pipes before confirmation
        if pattern or all_flag:
            click.echo(f"\nFound {len(pipes_to_process)} pipe(s) to drop and recreate:")
            for pipe_info in pipes_to_process:
                p_name = pipe_info[0] if isinstance(pipe_info, tuple) else pipe_info
                p_state = (
                    pipe_info[1]
                    if isinstance(pipe_info, tuple) and len(pipe_info) > 1
                    else "UNKNOWN"
                )
                state_color = _get_status_color(p_state)
                click.echo(
                    f"  - {Fore.CYAN}{p_name}{Style.RESET_ALL} (Status: {state_color}{p_state}{Style.RESET_ALL})"
                )

            if not click.confirm(
                f"\nAre you sure you want to DROP and RECREATE {len(pipes_to_process)} pipe(s)?"
            ):
                click.echo("Operation cancelled.")
                return
        else:
            # Single pipe confirmation
            if not click.confirm(
                f"Are you sure you want to DROP and RECREATE pipe '{pipe_name}'?"
            ):
                click.echo("Operation cancelled.")
                return

        for pipe_info in pipes_to_process:
            # Handle both tuple (name, state, database, schema) and string (name only) formats
            if isinstance(pipe_info, tuple):
                p_name = pipe_info[0]
                p_database = pipe_info[2] if len(pipe_info) > 2 else None
                p_schema = pipe_info[3] if len(pipe_info) > 3 else None
            else:
                p_name = pipe_info
                p_database = None
                p_schema = None

                # Parse schema/database from string input if present
                if "." in p_name:
                    parts = p_name.split(".")
                    if len(parts) == 3:
                        p_database, p_schema, p_name = parts
                    elif len(parts) == 2:
                        p_schema, p_name = parts

            try:
                # Build fully qualified pipe name
                pipe_schema = p_schema or target_schema
                pipe_database = p_database or target_database

                if pipe_database and pipe_schema:
                    full_pipe_name = f"{pipe_database}.{pipe_schema}.{p_name}"
                elif pipe_schema:
                    full_pipe_name = f"{pipe_schema}.{p_name}"
                else:
                    full_pipe_name = p_name

                # 1. Get DDL
                click.echo(f"Fetching DDL for pipe {p_name}...")
                ddl_query = f"SELECT GET_DDL('pipe', '{full_pipe_name}')"
                cursor = ConnectionManager.execute(ddl_query, conn_config=conn_config)
                res = cursor.fetchone()
                cursor.close()

                if not res:
                    raise click.ClickException(f"Could not fetch DDL for pipe {p_name}")
                ddl = res[0]

                if not ddl:
                    raise click.ClickException(f"Empty DDL returned for pipe {p_name}")

                # 2. Drop Pipe
                drop_query = f"DROP PIPE {full_pipe_name}"
                click.echo(f"Executing: {drop_query}")
                cursor = ConnectionManager.execute(drop_query, conn_config=conn_config)
                cursor.close()

                # 3. Recreate Pipe
                # DDL from GET_DDL often lacks fully qualified names.
                # Must set context to ensure creation happens in correct schema.
                if pipe_database and pipe_schema:
                    use_query = f"USE SCHEMA {pipe_database}.{pipe_schema}"
                    click.echo(f"Setting context: {use_query}")
                    cursor = ConnectionManager.execute(
                        use_query, conn_config=conn_config
                    )
                    cursor.close()

                click.echo(f"Recreating pipe {p_name}...")
                cursor = ConnectionManager.execute(ddl, conn_config=conn_config)
                cursor.close()
                click.echo(
                    f"{Fore.GREEN}Successfully recreated pipe: {p_name}{Style.RESET_ALL}"
                )

            except Exception as e:
                # FAIL FAST: Re-raise exception to stop processing loop
                click.echo(
                    f"{Fore.RED}Error processing pipe {p_name}: {e}{Style.RESET_ALL}",
                    err=True,
                )
                raise click.ClickException(
                    f"Aborting due to error on pipe {p_name}: {e}"
                )

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)
