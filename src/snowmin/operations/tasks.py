"""Task management operations for Snowflake"""

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


def list_tasks_command(
    ctx,
    match: Optional[str] = None,
    schema: Optional[str] = None,
):
    """List tasks"""
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

        query = "SHOW TASKS"
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
            f"Fetching tasks{f' from {target_database}.{target_schema}' if target_schema else f' from database {target_database}' if target_database else ''}..."
        )

        cursor = ConnectionManager.execute(query, conn_config=conn_config)

        # Helper to find column index case-insensitively
        col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
        name_idx = col_map.get("NAME")
        state_idx = col_map.get("STATE")
        schema_idx = col_map.get("SCHEMA_NAME")

        if name_idx is None:
            click.echo(
                f"{Fore.RED}Error: Could not find 'name' column in SHOW TASKS result.{Style.RESET_ALL}",
                err=True,
            )
            cursor.close()
            return

        rows = cursor.fetchall()
        cursor.close()

        if not rows:
            click.echo("No tasks found.")
            return

        click.echo(f"\nFound {len(rows)} tasks:")
        click.echo("-" * 60)

        for row in rows:
            t_name = row[name_idx]
            t_state = row[state_idx] if state_idx is not None else "UNKNOWN"
            t_schema = row[schema_idx] if schema_idx is not None else "UNKNOWN"

            if match and not re.search(match, t_name):
                continue

            state_color = Fore.GREEN if t_state == "started" else Fore.YELLOW
            click.echo(f"{t_schema}.{t_name}: {state_color}{t_state}{Style.RESET_ALL}")

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)


def _process_tasks(
    ctx,
    action: str,
    task_name: Optional[str],
    all_flag: bool,
    match: Optional[str],
    schema: Optional[str],
):
    """Helper to process task actions"""
    if not task_name and not all_flag and not match:
        raise click.UsageError("Must provide TASK_NAME, --all, or --match")

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

        tasks_to_process = []

        if task_name:
            tasks_to_process.append(task_name)
        elif all_flag or match:
            query = "SHOW TASKS"
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
                f"Fetching tasks{f' from {target_database}.{target_schema}' if target_schema else f' from database {target_database}' if target_database else ''}..."
            )
            cursor = ConnectionManager.execute(query, conn_config=conn_config)

            # Helper to find column index case-insensitively
            col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
            name_idx = col_map.get("NAME")
            state_idx = col_map.get("STATE")
            database_idx = col_map.get("DATABASE_NAME")
            schema_idx = col_map.get("SCHEMA_NAME")

            if name_idx is None:
                click.echo(
                    f"{Fore.RED}Error: Could not find 'name' column in SHOW TASKS result.{Style.RESET_ALL}",
                    err=True,
                )
                cursor.close()
                return

            rows = cursor.fetchall()
            cursor.close()

            for row in rows:
                t_name = row[name_idx]
                t_state = row[state_idx] if state_idx is not None else "UNKNOWN"
                t_database = row[database_idx] if database_idx is not None else None
                t_schema = row[schema_idx] if schema_idx is not None else None

                if match:
                    if re.search(match, t_name):
                        tasks_to_process.append((t_name, t_state, t_database, t_schema))
                else:
                    tasks_to_process.append((t_name, t_state, t_database, t_schema))

        if not tasks_to_process:
            click.echo("No tasks found to process.")
            return

        # Display tasks before confirmation
        if all_flag or match:
            click.echo(f"\nFound {len(tasks_to_process)} task(s) to {action}:")
            for task_info in tasks_to_process:
                t_name = task_info[0] if isinstance(task_info, tuple) else task_info
                t_state = (
                    task_info[1]
                    if isinstance(task_info, tuple) and len(task_info) > 1
                    else "UNKNOWN"
                )
                state_color = Fore.GREEN if t_state == "started" else Fore.YELLOW
                click.echo(
                    f"  - {Fore.CYAN}{t_name}{Style.RESET_ALL} (Status: {state_color}{t_state}{Style.RESET_ALL})"
                )

            if not click.confirm(
                f"\nAre you sure you want to {action.lower()} {len(tasks_to_process)} task(s)?"
            ):
                click.echo("Operation cancelled.")
                return

        for task_info in tasks_to_process:
            # Handle both tuple (name, state, database, schema) and string (name only) formats
            if isinstance(task_info, tuple):
                t_name = task_info[0]
                t_database = task_info[2] if len(task_info) > 2 else None
                t_schema = task_info[3] if len(task_info) > 3 else None
            else:
                t_name = task_info
                t_database = None
                t_schema = None

            try:
                # Build fully qualified task name
                # Priority: 1) schema from SHOW TASKS, 2) target_schema from --schema param
                task_schema = t_schema or target_schema
                task_database = t_database or target_database

                if task_database and task_schema:
                    full_task_name = f"{task_database}.{task_schema}.{t_name}"
                elif task_schema:
                    full_task_name = f"{task_schema}.{t_name}"
                else:
                    full_task_name = t_name

                query = f"ALTER TASK {full_task_name} {action}"
                click.echo(f"Executing: {query}")
                cursor = ConnectionManager.execute(query, conn_config=conn_config)
                cursor.close()
                click.echo(
                    f"{Fore.GREEN}Successfully {action}ED task: {t_name}{Style.RESET_ALL}"
                )
            except Exception as e:
                click.echo(
                    f"{Fore.RED}Error processing task {t_name}: {e}{Style.RESET_ALL}",
                    err=True,
                )

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)


def suspend_task_command(
    ctx,
    task_name: Optional[str],
    all: bool,
    match: Optional[str],
    schema: Optional[str],
):
    """Suspend a task or multiple tasks"""
    _process_tasks(ctx, "SUSPEND", task_name, all, match, schema)


def resume_task_command(
    ctx,
    task_name: Optional[str],
    all: bool,
    match: Optional[str],
    schema: Optional[str],
):
    """Resume a task or multiple tasks"""
    _process_tasks(ctx, "RESUME", task_name, all, match, schema)
