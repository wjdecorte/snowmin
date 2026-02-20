from typing import Dict, List
from snowmin.core.connection import ConnectionManager
from snowmin.core.state import Resource
from snowmin.resources.account import Warehouse


class Introspector:
    def __init__(self):
        self.conn = ConnectionManager

    def fetch_all(self) -> Dict[str, Resource]:
        """
        Fetch all managed resources from Snowflake and return as a dict of {identifier: Resource}.
        """
        resources = {}

        # Warehouses
        warehouses = self.fetch_warehouses()
        for wh in warehouses:
            resources[wh.identifier] = wh

        # Tables
        tables = self.fetch_tables()
        for t in tables:
            resources[t.identifier] = t

        # Add other types here...

        return resources

    def fetch_warehouses(self) -> List[Warehouse]:
        # SHOW WAREHOUSES check logic
        # Result columns: name, state, type, size, ...
        # exact columns depend on snowflake version/account
        # We generally execute "SHOW WAREHOUSES"
        try:
            rows = self.conn.fetch_all("SHOW WAREHOUSES")
            results = []
            for row in rows:
                # row keys are lowercase
                name = row["name"]
                size = row["size"]

                # auto_suspend might be in different column or need parsing
                # 'auto_suspend': 600
                auto_suspend = row.get("auto_suspend")
                if auto_suspend == "null" or auto_suspend is None:
                    auto_suspend = None
                else:
                    auto_suspend = int(auto_suspend)

                # 'auto_resume': 'true'
                auto_resume = str(row.get("auto_resume", "true")).lower() == "true"

                comment = row.get("comment", "")
                scaling_policy = row.get("scaling_policy", "STANDARD")

                wh = Warehouse(
                    name=name,
                    warehouse_size=size,
                    auto_suspend=auto_suspend,
                    auto_resume=auto_resume,
                    scaling_policy=scaling_policy,
                    comment=comment,
                )
                # clear registry for fetched objects?
                # fetched objects shouldn't auto-register as "Desired State".
                # My base class auto-registers. This is a problem.
                # Use a context manager or explicit flag?
                # Or just clear registry before loading user config?
                # User config is loaded AFTER introspection? usually "Plan" = Diff(Desired, Current)
                # Ideally 'Current' objects are just objects, not in "The Registry" of desired state.
                # So I need to hack/fix the base class auto-registry.
                results.append(wh)
            return results
        except Exception as e:
            # Maybe permission error or no warehouses
            print(f"Warning: Failed to fetch warehouses: {e}")
            return []

    def fetch_tables(self) -> List[Resource]:
        """Fetch all tables in the account (or reachable schemas)."""
        # SHOW OBJECTS? SHOW TABLES lacks columns.
        # Strategy:
        # 1. SHOW TABLES IN ACCOUNT
        # 2. Group by schema
        # 3. For each table, DESC TABLE (expensive!)
        # Optimization: Only DESC tables that are in our Desired State? (Reconciliation limit)
        # But we also want to detect drift/unmanaged tables (Destroy).
        # For now, let's just implement SHOW TABLES and assume we can DESC them.

        tables = []
        try:
            # This might be huge in real account. Limit to databases we care about?
            # SHOW TABLES IN ACCOUNT is safest for discovery.
            rows = self.conn.fetch_all("SHOW TABLES IN ACCOUNT")

            for row in rows:
                db_name = row["database_name"]
                schema_name = row["schema_name"]
                table_name = row["name"]
                comment = row.get("comment")

                # Fetch columns
                # We need fully qualified name
                full_name = f"{db_name}.{schema_name}.{table_name}"

                # TODO: Optimize this N+1 query.
                # Information Schema might be better:
                # SELECT * FROM snowflake.account_usage.columns WHERE deleted = NULL
                # But that has latency.
                # INFORMATION_SCHEMA in specific DB is faster but per-DB.

                # Let's try DESC TABLE for now.
                try:
                    cols_rows = self.conn.fetch_all(f"DESC TABLE {full_name}")
                    columns = []
                    for c_row in cols_rows:
                        # name, type, kind, null?, default, primary key, unique key...
                        # DESC output: name, type, kind, null?, default, primary key, ..
                        c_name = c_row["name"]
                        c_type = c_row["type"]  # e.g. VARCHAR(100), NUMBER(38,0)
                        c_null = c_row["null?"] == "Y"
                        c_comment = c_row.get("comment")

                        from snowmin.resources.schema_objects import Column

                        columns.append(
                            Column(
                                name=c_name,
                                type=c_type,
                                nullable=c_null,
                                comment=c_comment,
                            )
                        )

                    from snowmin.resources.schema_objects import Table

                    tables.append(
                        Table(
                            name=table_name,
                            database=db_name,
                            schema=schema_name,
                            columns=columns,
                            comment=comment,
                            register=False,
                        )
                    )
                except Exception as e:
                    print(f"Warning: Failed to describe table {full_name}: {e}")

        except Exception as e:
            print(f"Warning: Failed to fetch tables: {e}")

        return tables
