from typing import ClassVar
from pydantic import BaseModel, Field
from snowmin.core.registry import ResourceRegistry


class Resource(BaseModel):
    """
    Base class for all Snowflake resources.
    """

    name: str = Field(
        ...,
        description="Name of the resource (case-insensitive usually, but Snowflake is tricky)",
    )

    # Internal usage
    _snowflake_type: ClassVar[str] = "resource"

    def __init__(self, register: bool = True, **data):
        super().__init__(**data)
        if register:
            ResourceRegistry.register(self)

    @property
    def identifier(self) -> str:
        """Unique identifier for the resource (e.g. 'warehouse.my_wh')"""
        return f"{self._snowflake_type}.{self.name.upper()}"

    def get_create_sql(self) -> str:
        """Return SQL to create this resource."""
        raise NotImplementedError

    def get_alter_sql(self, current_state: "Resource") -> str:
        """Return SQL to alter from current_state to this (desired) state."""
        raise NotImplementedError

    def get_drop_sql(self) -> str:
        """Return SQL to drop this resource."""
        return f"DROP {self._snowflake_type.upper()} IF EXISTS {self.name}"


class AccountObject(Resource):
    """Resources that exist at the account level (Warehouse, Role, Database, etc.)"""

    pass


class SchemaObject(Resource):
    """Resources that exist within a schema (Table, View, etc.)"""

    database: str
    schema_name: str = Field(..., alias="schema")

    @property
    def identifier(self) -> str:
        return f"{self._snowflake_type}.{self.database.upper()}.{self.schema_name.upper()}.{self.name.upper()}"
