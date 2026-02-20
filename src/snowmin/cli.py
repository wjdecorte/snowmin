from snowmin.core.config import get_settings, get_merged_connection_config, CONFIG_FILE
import yaml
import click
import colorama
from colorama import Fore, Style

colorama.init()


@click.group()
@click.option(
    "--connection", "-c", help="Connection profile name from connections.toml"
)
@click.option("--database", help="Override database")
@click.option("--schema", help="Override schema")
@click.option("--warehouse", help="Override warehouse")
@click.option("--role", help="Override role")
@click.pass_context
def cli(ctx, connection, database, schema, warehouse, role):
    """Snowmin - Snowflake Infrastructure as Code"""
    ctx.ensure_object(dict)

    # Build CLI overrides dict for Settings init (highest priority via init_settings)
    cli_params = {}
    if connection:
        cli_params["connection"] = connection
    if database:
        cli_params["database"] = database
    if schema:
        cli_params["schema"] = schema
    if warehouse:
        cli_params["warehouse"] = warehouse
    if role:
        cli_params["role"] = role

    # Load settings with CLI params (Pydantic will merge: CLI > env > env config > generic config)
    try:
        settings = get_settings(**cli_params)
    except Exception:
        # If no config file exists yet, create minimal settings
        from snowmin.core.config import Settings

        settings = Settings(**cli_params)

    # Build CLI overrides dict for get_merged_connection_config (for backward compatibility)
    cli_overrides = {}
    if connection:
        cli_overrides["connection"] = connection
    if database:
        cli_overrides["database"] = database
    if schema:
        cli_overrides["schema"] = schema
    if warehouse:
        cli_overrides["warehouse"] = warehouse
    if role:
        cli_overrides["role"] = role

    # Store in context for commands to use
    ctx.obj["settings"] = settings
    ctx.obj["cli_overrides"] = cli_overrides

    # Get merged config for display
    try:
        merged_config = get_merged_connection_config(settings, cli_overrides)
        if merged_config.get("database"):
            click.echo(
                f"Current database: {Fore.CYAN}{merged_config['database']}{Style.RESET_ALL}"
            )
    except Exception:
        # Config might not be complete yet, that's OK
        pass


@cli.group()
def config():
    """Manage configuration"""
    pass


@config.command()
@click.pass_context
def show(ctx):
    """Show current configuration"""
    try:
        settings = ctx.obj["settings"]
        cli_overrides = ctx.obj["cli_overrides"]

        # Show snowmin settings
        click.echo(f"{Fore.CYAN}Snowmin Settings:{Style.RESET_ALL}")
        data = settings.model_dump(mode="json", exclude_none=True)
        click.echo(yaml.dump(data, default_flow_style=False))
        click.echo(f"Loaded from: {CONFIG_FILE}")

        # Show merged connection config
        click.echo(f"\n{Fore.CYAN}Merged Connection Config:{Style.RESET_ALL}")
        try:
            merged_config = get_merged_connection_config(settings, cli_overrides)
            # Mask sensitive fields
            for key in ["password", "private_key_file", "private_key_passphrase"]:
                if key in merged_config and merged_config[key]:
                    merged_config[key] = "******"
            click.echo(yaml.dump(merged_config, default_flow_style=False))
        except Exception as e:
            click.echo(f"Error loading connection config: {e}")

    except Exception as e:
        click.echo(f"Error loading config: {e}")


@config.command()
@click.argument("key")
@click.argument("value")
def set(key, value):
    """Set a configuration value in ~/.snowmin/config.yaml"""
    try:
        # Load existing raw config to preserve unset optional fields
        if CONFIG_FILE.exists():
            with open(CONFIG_FILE, "r") as f:
                raw_config = yaml.safe_load(f) or {}
        else:
            raw_config = {}

        from snowmin.core.config import Settings

        # Basic validation: check if key exists in model
        if key not in Settings.model_fields:
            click.echo(f"Warning: '{key}' is not a known configuration setting.")

        raw_config[key] = value

        # Write back
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_FILE, "w") as f:
            yaml.dump(raw_config, f)

        click.echo(f"Updated {key} = {value}")

    except Exception as e:
        click.echo(f"Error updating config: {e}")


@cli.command()
@click.option(
    "--stack",
    "-s",
    default="stack.py",
    show_default=True,
    help="Path to the stack Python file defining resources to deploy",
)
@click.pass_context
def plan(ctx, stack):
    """Show changes required to reach desired state"""
    from snowmin.core.stack_loader import load_stack
    from snowmin.core.runner import Runner

    load_stack(stack)

    runner = Runner()
    plan_sql = runner.plan()

    if not plan_sql:
        click.echo("No changes detected.")
    else:
        click.echo("\nProposed Changes:")
        for sql in plan_sql:
            click.echo(sql)


@cli.command()
@click.option(
    "--stack",
    "-s",
    default="stack.py",
    show_default=True,
    help="Path to the stack Python file defining resources to deploy",
)
@click.pass_context
def apply(ctx, stack):
    """Apply changes to Snowflake"""
    from snowmin.core.stack_loader import load_stack
    from snowmin.core.runner import Runner

    load_stack(stack)

    runner = Runner()
    plan_sql = runner.plan()

    if plan_sql:
        if click.confirm("Do you want to apply these changes?"):
            runner.apply(plan_sql)
        else:
            click.echo("Apply cancelled.")
    else:
        click.echo("No changes to apply.")


@cli.command()
def destroy():
    """Destroy managed infrastructure"""
    click.echo("Destroy command not implemented yet")


@cli.command()
def import_cmd():
    """Import existing Snowflake objects"""
    click.echo("Import command not implemented yet")


@cli.group()
@click.pass_context
def tasks(ctx):
    """Manage Snowflake Tasks"""
    pass


@tasks.command("list")
@click.option("--match", help="List tasks matching regex pattern")
@click.option("--schema", help="Schema to look for tasks in")
@click.pass_context
def list_tasks(ctx, match, schema):
    """List tasks"""
    from snowmin.operations.tasks import list_tasks_command

    list_tasks_command(ctx, match, schema)


@tasks.command("suspend")
@click.argument("task_name", required=False)
@click.option("--all", is_flag=True, help="Suspend all tasks in schema")
@click.option("--match", help="Suspend tasks matching regex pattern")
@click.option("--schema", help="Schema to look for tasks in")
@click.pass_context
def suspend_task(ctx, task_name, all, match, schema):
    """Suspend a task or multiple tasks"""
    from snowmin.operations.tasks import suspend_task_command

    suspend_task_command(ctx, task_name, all, match, schema)


@tasks.command("resume")
@click.argument("task_name", required=False)
@click.option("--all", is_flag=True, help="Resume all tasks in schema")
@click.option("--match", help="Resume tasks matching regex pattern")
@click.option("--schema", help="Schema to look for tasks in")
@click.pass_context
def resume_task(ctx, task_name, all, match, schema):
    """Resume a task or multiple tasks"""
    from snowmin.operations.tasks import resume_task_command

    resume_task_command(ctx, task_name, all, match, schema)


@cli.group()
@click.pass_context
def tables(ctx):
    """Manage Snowflake Tables"""
    pass


@tables.command("truncate")
@click.argument("table_name")
@click.pass_context
def truncate_table(ctx, table_name):
    """Truncate a table"""
    from snowmin.operations.tables import truncate_table_command

    truncate_table_command(ctx, table_name)


@cli.group()
@click.pass_context
def pipes(ctx):
    """Manage Snowflake Pipes"""
    pass


@pipes.command("list")
@click.option("--pattern", help="Filter pipes by regex pattern")
@click.option("--schema", help="Schema to query (DATABASE.SCHEMA or SCHEMA)")
@click.option("--status", help="Filter by status (RUNNING, PAUSED, STALLED)")
@click.pass_context
def list_pipes(ctx, pattern, schema, status):
    """List pipes"""
    from snowmin.operations.pipes import list_pipes_command

    list_pipes_command(ctx, pattern, schema, status)


@pipes.command("refresh")
@click.argument("pipe_name", required=False)
@click.option("--pattern", help="Refresh pipes matching regex pattern")
@click.option("--schema", help="Schema to query")
@click.option("--status", help="Filter by status before refreshing")
@click.pass_context
def refresh_pipe(ctx, pipe_name, pattern, schema, status):
    """Refresh a pipe or multiple pipes"""
    from snowmin.operations.pipes import refresh_pipe_command

    refresh_pipe_command(ctx, pipe_name, pattern, schema, status)


@pipes.command("pause")
@click.argument("pipe_name", required=False)
@click.option("--pattern", help="Pause pipes matching regex pattern")
@click.option("--schema", help="Schema to query")
@click.option("--status", help="Filter by status before pausing")
@click.pass_context
def pause_pipe(ctx, pipe_name, pattern, schema, status):
    """Pause a pipe or multiple pipes"""
    from snowmin.operations.pipes import pause_pipe_command

    pause_pipe_command(ctx, pipe_name, pattern, schema, status)


@pipes.command("resume")
@click.argument("pipe_name", required=False)
@click.option("--pattern", help="Resume pipes matching regex pattern")
@click.option("--schema", help="Schema to query")
@click.option("--status", help="Filter by status before resuming")
@click.pass_context
def resume_pipe(ctx, pipe_name, pattern, schema, status):
    """Resume a pipe or multiple pipes"""
    from snowmin.operations.pipes import resume_pipe_command

    resume_pipe_command(ctx, pipe_name, pattern, schema, status)


@pipes.command("drop-recreate")
@click.argument("pipe_name", required=False)
@click.option("--all", is_flag=True, help="Process all pipes")
@click.option("--pattern", help="Drop-recreate pipes matching regex pattern")
@click.option("--schema", help="Schema to query")
@click.option("--status", help="Filter by status before drop-recreate")
@click.option(
    "--skip-status",
    is_flag=True,
    help="Skip fetching current status (faster, but status will be UNKNOWN)",
)
@click.pass_context
def drop_recreate_pipe(ctx, pipe_name, all, pattern, schema, status, skip_status):
    """Drop and recreate one or more pipes using their current DDL"""
    from snowmin.operations.pipes import drop_recreate_pipe_command

    drop_recreate_pipe_command(
        ctx, pipe_name, all, pattern, schema, status, skip_status
    )


if __name__ == "__main__":
    cli()
