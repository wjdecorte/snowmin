from typing import List

import click
from snowmin.core.introspector import Introspector
from snowmin.core.registry import ResourceRegistry
from snowmin.core.connection import ConnectionManager


class Runner:
    def __init__(self):
        self.introspector = Introspector()
        self.conn = ConnectionManager

    def plan(self) -> List[str]:
        """
        Compare desired state (Registry) with current state (Introspector).
        Returns list of SQL statements.
        """
        # 1. Fetch current state
        click.echo("Fetching current state from Snowflake...")
        current_state = self.introspector.fetch_all()

        # 2. Get desired state
        desired_state = ResourceRegistry.get_all()

        plan_sql = []

        from colorama import Fore, Style

        # 3. Compare Desired vs Current
        for resource in desired_state:
            identifier = resource.identifier
            if identifier not in current_state:
                # Create
                click.echo(f"  {Fore.GREEN}+ Create {identifier}{Style.RESET_ALL}")
                plan_sql.append(resource.get_create_sql())
            else:
                # Update?
                current = current_state[identifier]
                alter_sql = resource.get_alter_sql(current)
                if alter_sql:
                    click.echo(f"  {Fore.YELLOW}~ Update {identifier}{Style.RESET_ALL}")
                    plan_sql.append(alter_sql)

        # 4. Check for Destructive changes (Resources in Current but not Desired)
        # Note: We should be careful. Is Desired State authoritative?
        # If so, drop everything else.
        # But we might only be managing a subset.
        # For this MVP, let's assume authoritative for the types we support.
        # We need to know which types we support to avoiding dropping things we don't know about.
        # Since Introspector only fetches what we support (e.g. Warehouses),
        # checking "if identifier in current but not desired" is safe-ish for those types.

        desired_identifiers = {r.identifier for r in desired_state}
        for identifier, resource in current_state.items():
            if identifier not in desired_identifiers:
                # Drop
                click.echo(f"  {Fore.RED}- Destroy {identifier}{Style.RESET_ALL}")
                plan_sql.append(resource.get_drop_sql())

        return plan_sql

    def apply(self, plan_sql: List[str]):
        """Execute the plan."""
        if not plan_sql:
            click.echo("No changes to apply.")
            return

        click.echo("\nApplying changes...")
        for sql in plan_sql:
            click.echo(f"Executing: {sql}")
            self.conn.execute(sql)
        click.echo("Apply complete.")
