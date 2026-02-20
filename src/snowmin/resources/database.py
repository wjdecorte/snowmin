from typing import Optional
from pydantic import Field
from snowmin.core.state import AccountObject, Resource


class Database(AccountObject):
    _snowflake_type = "database"

    comment: Optional[str] = Field(None)
    data_retention_time_in_days: Optional[int] = Field(
        None, description="Data retention in days"
    )

    def get_create_sql(self) -> str:
        sql = f"CREATE DATABASE {self.name}"
        if self.data_retention_time_in_days is not None:
            sql += f" DATA_RETENTION_TIME_IN_DAYS = {self.data_retention_time_in_days}"
        if self.comment:
            sql += f" COMMENT = '{self.comment}'"
        return sql

    def get_alter_sql(self, current_state: "Database") -> str:
        changes = []
        if (
            self.data_retention_time_in_days
            != current_state.data_retention_time_in_days
        ):
            changes.append(
                f"DATA_RETENTION_TIME_IN_DAYS = {self.data_retention_time_in_days}"
            )
        if self.comment != current_state.comment:
            changes.append(f"COMMENT = '{self.comment}'")

        if not changes:
            return ""
        return f"ALTER DATABASE {self.name} SET " + ", ".join(changes)


class Schema(Resource):
    _snowflake_type = "schema"

    comment: Optional[str] = Field(None)
    data_retention_time_in_days: Optional[int] = Field(None)
    managed_access: bool = Field(False, description="WITH MANAGED ACCESS")

    # Parent database
    database: str = Field(..., description="Parent database")

    def add_table(self, model_class):
        """
        Register a Pydantic model as a Table in this schema.
        """
        from snowmin.resources.schema_objects import Table

        # Instantiate Table, which auto-registers with ResourceRegistry
        Table.from_model(database=self.database, schema=self.name, model=model_class)

    @property
    def identifier(self) -> str:
        return f"{self._snowflake_type}.{self.database.upper()}.{self.name.upper()}"

    def get_create_sql(self) -> str:
        sql = f"CREATE SCHEMA {self.database}.{self.name}"
        if self.managed_access:
            sql += " WITH MANAGED ACCESS"
        if self.data_retention_time_in_days is not None:
            sql += f" DATA_RETENTION_TIME_IN_DAYS = {self.data_retention_time_in_days}"
        if self.comment:
            sql += f" COMMENT = '{self.comment}'"
        return sql

    def get_alter_sql(self, current_state: "Schema") -> str:
        changes = []
        if (
            self.data_retention_time_in_days
            != current_state.data_retention_time_in_days
        ):
            changes.append(
                f"DATA_RETENTION_TIME_IN_DAYS = {self.data_retention_time_in_days}"
            )
        if self.comment != current_state.comment:
            changes.append(f"COMMENT = '{self.comment}'")

        if not changes:
            return ""
        return f"ALTER SCHEMA {self.database}.{self.name} SET " + ", ".join(changes)
