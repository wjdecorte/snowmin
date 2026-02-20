"""Table management operations for Snowflake"""

import click
from colorama import Fore, Style
from snowmin.core.connection import ConnectionManager
from snowmin.core.config import get_merged_connection_config


def truncate_table_command(ctx, table_name: str):
    """Truncate a table"""
    if not click.confirm(
        f"Are you sure you want to TRUNCATE table '{table_name}'? This deletes all rows."
    ):
        click.echo("Operation cancelled.")
        return

    settings = ctx.obj["settings"]
    cli_overrides = ctx.obj["cli_overrides"]
    conn_config = get_merged_connection_config(settings, cli_overrides)

    try:
        query = f"TRUNCATE TABLE {table_name}"
        click.echo(f"Executing: {query}")
        cursor = ConnectionManager.execute(query, conn_config=conn_config)
        cursor.close()
        click.echo(
            f"{Fore.GREEN}Successfully truncated table: {table_name}{Style.RESET_ALL}"
        )
    except Exception as e:
        click.echo(f"{Fore.RED}Error truncating table: {e}{Style.RESET_ALL}", err=True)
