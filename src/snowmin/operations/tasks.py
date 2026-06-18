"""Task management operations for Snowflake"""

import json
import re
import click
from typing import Any, Optional
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


def _desired_state_for_action(action: str) -> str:
    """Return the only state a task should be in before applying action."""
    if action == "SUSPEND":
        return "started"
    if action == "RESUME":
        return "suspended"
    raise ValueError(f"Unknown task action: {action}")


def _action_past_tense(action: str) -> str:
    """Return a display-friendly past-tense task action."""
    if action == "SUSPEND":
        return "suspended"
    if action == "RESUME":
        return "resumed"
    return action.lower()


def _parse_predecessors(raw_predecessors: Any) -> list[str]:
    """Parse Snowflake's predecessors metadata into a list of task names."""
    if raw_predecessors is None:
        return []
    if isinstance(raw_predecessors, (list, tuple)):
        return [str(item) for item in raw_predecessors if item]
    if not isinstance(raw_predecessors, str):
        return [str(raw_predecessors)]

    value = raw_predecessors.strip()
    if not value or value == "[]":
        return []

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return [value]

    if isinstance(parsed, list):
        return [str(item) for item in parsed if item]
    if parsed:
        return [str(parsed)]
    return []


def _parse_task_relations(raw_task_relations: Any) -> dict[str, Any]:
    """Parse Snowflake's task_relations metadata."""
    if raw_task_relations is None:
        return {}
    if isinstance(raw_task_relations, dict):
        return raw_task_relations
    if not isinstance(raw_task_relations, str):
        return {}

    value = raw_task_relations.strip()
    if not value:
        return {}

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}

    return parsed if isinstance(parsed, dict) else {}


def _relation_value(relations: dict[str, Any], name: str):
    """Return a relation value regardless of Snowflake's key casing."""
    for key, value in relations.items():
        if key.lower() == name.lower():
            return value
    return None


def _full_task_name(t_name, t_database, t_schema, target_database, target_schema):
    """Build a fully qualified task name from SHOW TASKS metadata when available."""
    task_schema = t_schema or target_schema
    task_database = t_database or target_database

    if task_database and task_schema:
        return f"{task_database}.{task_schema}.{t_name}"
    if task_schema:
        return f"{task_schema}.{t_name}"
    return t_name


def _task_key(task_name: str) -> str:
    """Normalize task names for in-memory graph lookups."""
    return task_name.upper()


def _task_leaf_name(task_name: str) -> str:
    """Return the unqualified task name for fallback graph lookups."""
    return task_name.split(".")[-1]


def _build_task_info(
    row,
    name_idx,
    state_idx,
    database_idx,
    schema_idx,
    predecessors_idx,
    task_relations_idx,
    target_database,
    target_schema,
):
    """Build normalized task metadata from a SHOW TASKS row."""
    t_name = row[name_idx]
    t_state = row[state_idx] if state_idx is not None else "UNKNOWN"
    t_database = row[database_idx] if database_idx is not None else target_database
    t_schema = row[schema_idx] if schema_idx is not None else target_schema
    t_predecessors = (
        _parse_predecessors(row[predecessors_idx])
        if predecessors_idx is not None
        else []
    )
    task_relations = (
        _parse_task_relations(row[task_relations_idx])
        if task_relations_idx is not None
        else {}
    )
    relation_predecessors = _parse_predecessors(
        _relation_value(task_relations, "Predecessors")
    )
    if relation_predecessors:
        t_predecessors = relation_predecessors

    finalized_root_task = _relation_value(task_relations, "FinalizedRootTask")
    finalizer_task = _relation_value(task_relations, "FinalizerTask")
    full_name = _full_task_name(
        t_name, t_database, t_schema, target_database, target_schema
    )

    return {
        "name": t_name,
        "state": t_state,
        "database": t_database,
        "schema": t_schema,
        "predecessors": t_predecessors,
        "finalized_root_task": finalized_root_task,
        "finalizer_task": finalizer_task,
        "full_name": full_name,
    }


def _load_discovered_tasks(query, conn_config, target_database, target_schema):
    """Load normalized task metadata from a SHOW TASKS query."""
    cursor = ConnectionManager.execute(query, conn_config=conn_config)
    col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
    name_idx = col_map.get("NAME")
    state_idx = col_map.get("STATE")
    database_idx = col_map.get("DATABASE_NAME")
    schema_idx = col_map.get("SCHEMA_NAME")
    predecessors_idx = col_map.get("PREDECESSORS")
    task_relations_idx = col_map.get("TASK_RELATIONS")

    if name_idx is None:
        cursor.close()
        raise click.ClickException("Could not find 'name' column in SHOW TASKS result.")

    rows = cursor.fetchall()
    cursor.close()

    return [
        _build_task_info(
            row,
            name_idx,
            state_idx,
            database_idx,
            schema_idx,
            predecessors_idx,
            task_relations_idx,
            target_database,
            target_schema,
        )
        for row in rows
    ]


def _graph_roots_for_task(task_info, task_lookup, visited=None):
    """Find root task(s) for a task graph using predecessor metadata."""
    if visited is None:
        visited = set()

    full_name = task_info["full_name"]
    key = _task_key(full_name)
    if key in visited:
        return {full_name}
    visited.add(key)

    predecessors = list(task_info["predecessors"])
    if task_info["finalized_root_task"]:
        predecessors.append(task_info["finalized_root_task"])
    if not predecessors:
        return {full_name}

    roots = set()
    for predecessor in predecessors:
        predecessor_info = task_lookup.get(_task_key(predecessor))
        if predecessor_info is None:
            predecessor_info = task_lookup.get(_task_key(_task_leaf_name(predecessor)))

        if predecessor_info is None:
            roots.add(predecessor)
        else:
            roots.update(_graph_roots_for_task(predecessor_info, task_lookup, visited))

    return roots


def _build_graph_context(discovered_tasks):
    """Build lookup tables for task graph planning."""
    task_lookup = {}
    children_by_task = {}

    for task_info in discovered_tasks:
        task_lookup[_task_key(task_info["full_name"])] = task_info
        task_lookup[_task_key(task_info["name"])] = task_info
        for predecessor in task_info["predecessors"]:
            children_by_task.setdefault(_task_key(predecessor), set()).add(
                task_info["full_name"]
            )
            children_by_task.setdefault(
                _task_key(_task_leaf_name(predecessor)), set()
            ).add(task_info["full_name"])
        if task_info["finalized_root_task"]:
            root_task = task_info["finalized_root_task"]
            children_by_task.setdefault(_task_key(root_task), set()).add(
                task_info["full_name"]
            )
            children_by_task.setdefault(
                _task_key(_task_leaf_name(root_task)), set()
            ).add(task_info["full_name"])
        if task_info["finalizer_task"]:
            children_by_task.setdefault(_task_key(task_info["full_name"]), set()).add(
                task_info["finalizer_task"]
            )

    return task_lookup, children_by_task


def _is_graph_member(task_info, children_by_task):
    """Return whether a task belongs to a task graph."""
    return (
        bool(task_info["predecessors"])
        or bool(task_info["finalized_root_task"])
        or bool(task_info["finalizer_task"])
        or bool(children_by_task.get(_task_key(task_info["full_name"])))
    )


def _plan_suspend_statements(tasks_to_process, discovered_tasks):
    """Build graph-aware SUSPEND statements with roots first."""
    task_lookup, children_by_task = _build_graph_context(discovered_tasks)

    statements = []
    planned_roots = set()
    suspended_roots = set()
    planned_tasks = set()

    for task_info in tasks_to_process:
        if not _is_graph_member(task_info, children_by_task):
            planned_tasks.add(_task_key(task_info["full_name"]))
            statements.append(
                {
                    "task": task_info,
                    "queries": [f"ALTER TASK {task_info['full_name']} SUSPEND"],
                    "kind": "task",
                    "display_name": task_info["name"],
                }
            )
            continue

        for root_name in sorted(
            _graph_roots_for_task(task_info, task_lookup), key=str.upper
        ):
            root_key = _task_key(root_name)
            if root_key in planned_roots:
                continue
            planned_roots.add(root_key)
            suspended_roots.add(root_key)
            statements.append(
                {
                    "task": task_info,
                    "queries": [f"ALTER TASK {root_name} SUSPEND"],
                    "kind": "graph-root",
                    "display_name": root_name,
                }
            )

    for task_info in tasks_to_process:
        task_key = _task_key(task_info["full_name"])
        if task_key in suspended_roots or task_key in planned_tasks:
            continue
        statements.append(
            {
                "task": task_info,
                "queries": [f"ALTER TASK {task_info['full_name']} SUSPEND"],
                "kind": "task",
                "display_name": task_info["name"],
            }
        )

    return statements


def _plan_resume_statements(tasks_to_process, discovered_tasks):
    """Build graph-aware RESUME statements."""
    task_lookup, children_by_task = _build_graph_context(discovered_tasks)

    statements = []
    planned_roots = set()

    for task_info in tasks_to_process:
        if not _is_graph_member(task_info, children_by_task):
            statements.append(
                {
                    "task": task_info,
                    "queries": [f"ALTER TASK {task_info['full_name']} RESUME"],
                    "kind": "task",
                    "display_name": task_info["name"],
                }
            )
            continue

        for root_name in sorted(
            _graph_roots_for_task(task_info, task_lookup), key=str.upper
        ):
            root_key = _task_key(root_name)
            if root_key in planned_roots:
                continue
            planned_roots.add(root_key)
            statements.append(
                {
                    "task": task_info,
                    "queries": [
                        f"ALTER TASK {root_name} SUSPEND",
                        f"SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('{root_name}')",
                    ],
                    "kind": "graph",
                    "display_name": root_name,
                }
            )

    return statements


def list_tasks_command(
    ctx,
    pattern: Optional[str] = None,
    schema: Optional[str] = None,
    status: Optional[str] = None,
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

        query = "SHOW TASKS" + _build_schema_query_suffix(
            target_database, target_schema
        )

        click.echo(
            f"Fetching tasks{_location_label(target_database, target_schema)}..."
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

        # Apply filters
        filtered = []
        for row in rows:
            t_name = row[name_idx]
            t_state = row[state_idx] if state_idx is not None else "UNKNOWN"
            t_schema = row[schema_idx] if schema_idx is not None else "UNKNOWN"

            if pattern and not re.search(pattern, t_name):
                continue
            if status and t_state.lower() != status.lower():
                continue

            filtered.append((t_schema, t_name, t_state))

        if not filtered:
            click.echo("No tasks matched the given filters.")
            return

        click.echo(f"\nFound {len(filtered)} task(s):")
        click.echo("-" * 60)

        for t_schema, t_name, t_state in filtered:
            state_color = Fore.GREEN if t_state == "started" else Fore.YELLOW
            click.echo(f"{t_schema}.{t_name}: {state_color}{t_state}{Style.RESET_ALL}")

    except Exception as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)


def _process_tasks(
    ctx,
    action: str,
    task_name: Optional[str],
    all_flag: bool,
    pattern: Optional[str],
    schema: Optional[str],
):
    """Helper to process task actions"""
    if not task_name and not all_flag and not pattern:
        raise click.UsageError("Must provide TASK_NAME, --all, or --pattern")

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
        discovered_tasks = []

        desired_state = _desired_state_for_action(action)

        if task_name:
            lookup_name = task_name
            lookup_database = target_database
            lookup_schema = target_schema
            if "." in lookup_name:
                parts = lookup_name.split(".")
                if len(parts) == 3:
                    lookup_database, lookup_schema, lookup_name = parts
                elif len(parts) == 2:
                    lookup_schema, lookup_name = parts

            query = f"SHOW TASKS LIKE '{lookup_name}'" + _build_schema_query_suffix(
                lookup_database, lookup_schema
            )
            cursor = ConnectionManager.execute(query, conn_config=conn_config)
            col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
            name_idx = col_map.get("NAME")
            state_idx = col_map.get("STATE")
            database_idx = col_map.get("DATABASE_NAME")
            schema_idx = col_map.get("SCHEMA_NAME")
            predecessors_idx = col_map.get("PREDECESSORS")
            task_relations_idx = col_map.get("TASK_RELATIONS")
            rows = cursor.fetchall()
            cursor.close()

            if name_idx is None:
                click.echo(
                    f"{Fore.RED}Error: Could not find 'name' column in SHOW TASKS result.{Style.RESET_ALL}",
                    err=True,
                )
                return
            if not rows:
                click.echo(f"No task found matching {task_name}.")
                return

            row = rows[0]
            task_info = _build_task_info(
                row,
                name_idx,
                state_idx,
                database_idx,
                schema_idx,
                predecessors_idx,
                task_relations_idx,
                lookup_database,
                lookup_schema,
            )
            discovered_tasks.append(task_info)
            t_state = task_info["state"]

            if str(t_state).lower() != desired_state:
                click.echo(
                    f"Task {task_name} is {t_state}, skipping "
                    f"(only {desired_state} tasks are {_action_past_tense(action)})."
                )
                return

            tasks_to_process.append(task_info)
            if (
                action in {"RESUME", "SUSPEND"}
                and (predecessors_idx is not None or task_relations_idx is not None)
                and task_info["database"]
                and task_info["schema"]
            ):
                graph_query = "SHOW TASKS" + _build_schema_query_suffix(
                    task_info["database"], task_info["schema"]
                )
                discovered_tasks = _load_discovered_tasks(
                    graph_query,
                    conn_config,
                    task_info["database"],
                    task_info["schema"],
                )
        elif all_flag or pattern:
            query = "SHOW TASKS" + _build_schema_query_suffix(
                target_database, target_schema
            )

            click.echo(
                f"Fetching tasks{_location_label(target_database, target_schema)}..."
            )
            cursor = ConnectionManager.execute(query, conn_config=conn_config)

            # Helper to find column index case-insensitively
            col_map = {c[0].upper(): i for i, c in enumerate(cursor.description)}
            name_idx = col_map.get("NAME")
            state_idx = col_map.get("STATE")
            database_idx = col_map.get("DATABASE_NAME")
            schema_idx = col_map.get("SCHEMA_NAME")
            predecessors_idx = col_map.get("PREDECESSORS")
            task_relations_idx = col_map.get("TASK_RELATIONS")

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
                task_info = _build_task_info(
                    row,
                    name_idx,
                    state_idx,
                    database_idx,
                    schema_idx,
                    predecessors_idx,
                    task_relations_idx,
                    target_database,
                    target_schema,
                )
                discovered_tasks.append(task_info)
                t_name = task_info["name"]
                t_state = task_info["state"]

                if pattern:
                    if re.search(pattern, t_name):
                        if str(t_state).lower() == desired_state:
                            tasks_to_process.append(task_info)
                else:
                    if str(t_state).lower() == desired_state:
                        tasks_to_process.append(task_info)

        if not tasks_to_process:
            click.echo(f"No {desired_state} tasks found to {action.lower()}.")
            return

        # Display tasks before confirmation
        if all_flag or pattern:
            click.echo(f"\nFound {len(tasks_to_process)} task(s) to {action}:")
            for task_info in tasks_to_process:
                t_name = task_info["name"]
                t_state = task_info["state"]
                state_color = Fore.GREEN if t_state == "started" else Fore.YELLOW
                click.echo(
                    f"  - {Fore.CYAN}{t_name}{Style.RESET_ALL} (Status: {state_color}{t_state}{Style.RESET_ALL})"
                )

            if not click.confirm(
                f"\nAre you sure you want to {action.lower()} {len(tasks_to_process)} task(s)?"
            ):
                click.echo("Operation cancelled.")
                return

        if action == "RESUME":
            statements = _plan_resume_statements(tasks_to_process, discovered_tasks)
        elif action == "SUSPEND":
            statements = _plan_suspend_statements(tasks_to_process, discovered_tasks)
        else:
            statements = [
                {
                    "task": task_info,
                    "queries": [f"ALTER TASK {task_info['full_name']} {action}"],
                    "kind": "task",
                    "display_name": task_info["name"],
                }
                for task_info in tasks_to_process
            ]

        for statement in statements:
            task_info = statement["task"]
            t_name = task_info["name"]
            try:
                for query in statement["queries"]:
                    click.echo(f"Executing: {query}")
                    cursor = ConnectionManager.execute(query, conn_config=conn_config)
                    cursor.close()
                if statement["kind"] == "graph":
                    click.echo(
                        f"{Fore.GREEN}Successfully resumed task graph rooted at: "
                        f"{statement['display_name']}{Style.RESET_ALL}"
                    )
                    continue
                if statement["kind"] == "graph-root":
                    click.echo(
                        f"{Fore.GREEN}Successfully suspended task graph root: "
                        f"{statement['display_name']}{Style.RESET_ALL}"
                    )
                    continue

                click.echo(
                    f"{Fore.GREEN}Successfully {_action_past_tense(action)} task: {t_name}{Style.RESET_ALL}"
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
    pattern: Optional[str],
    schema: Optional[str],
):
    """Suspend a task or multiple tasks"""
    _process_tasks(ctx, "SUSPEND", task_name, all, pattern, schema)


def resume_task_command(
    ctx,
    task_name: Optional[str],
    all: bool,
    pattern: Optional[str],
    schema: Optional[str],
):
    """Resume a task or multiple tasks"""
    _process_tasks(ctx, "RESUME", task_name, all, pattern, schema)
