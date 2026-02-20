from typing import Optional

from pydantic import Field, validator
from snowmin.core.state import AccountObject, Resource


class Warehouse(AccountObject):
    _snowflake_type = "warehouse"

    # Common warehouse properties
    warehouse_size: Optional[str] = Field(
        None, description="Size of the warehouse (X-Small, Small, etc.)"
    )
    auto_suspend: Optional[int] = Field(600, description="Auto suspend time in seconds")
    auto_resume: Optional[bool] = Field(True, description="Auto resume when accessed")
    scaling_policy: Optional[str] = Field(
        "STANDARD", description="Scaling policy (STANDARD or ECONOMY)"
    )
    comment: Optional[str] = Field(None, description="Comment")

    @validator("warehouse_size")
    def validate_size(cls, v):
        if v:
            return v.upper()
        return v

    def get_create_sql(self) -> str:
        sql = f"CREATE WAREHOUSE {self.name}"
        if self.warehouse_size:
            sql += f" WAREHOUSE_SIZE = '{self.warehouse_size}'"
        if self.auto_suspend is not None:
            sql += f" AUTO_SUSPEND = {self.auto_suspend}"
        if self.auto_resume is not None:
            sql += f" AUTO_RESUME = {str(self.auto_resume).upper()}"
        if self.scaling_policy:
            sql += f" SCALING_POLICY = '{self.scaling_policy}'"
        if self.comment:
            sql += f" COMMENT = '{self.comment}'"
        return sql

    def get_alter_sql(self, current_state: "Warehouse") -> str:
        changes = []
        if self.warehouse_size != current_state.warehouse_size:
            # If desired is None, do we unset? Snowflake typically keeps previous value if not set in ALTER unless explicitly UNSET.
            # In declarative model, "None" might mean "don't care" or "default".
            # For strict declarative, we should probably set everything.
            # Assuming explicitly set values in 'self' are enforced.
            if self.warehouse_size:
                changes.append(f"WAREHOUSE_SIZE = '{self.warehouse_size}'")

        if self.auto_suspend != current_state.auto_suspend:
            changes.append(f"AUTO_SUSPEND = {self.auto_suspend}")

        if self.auto_resume != current_state.auto_resume:
            changes.append(f"AUTO_RESUME = {str(self.auto_resume).upper()}")

        if self.scaling_policy != current_state.scaling_policy:
            changes.append(f"SCALING_POLICY = '{self.scaling_policy}'")

        if self.comment != current_state.comment:
            changes.append(f"COMMENT = '{self.comment}'")

        if not changes:
            return ""

        return f"ALTER WAREHOUSE {self.name} SET " + ", ".join(changes)


class Role(AccountObject):
    _snowflake_type = "role"
    comment: Optional[str] = Field(None)

    def get_create_sql(self) -> str:
        sql = f"CREATE ROLE {self.name}"
        if self.comment:
            sql += f" COMMENT = '{self.comment}'"
        return sql

    def get_alter_sql(self, current_state: "Role") -> str:
        if self.comment != current_state.comment:
            return f"ALTER ROLE {self.name} SET COMMENT = '{self.comment}'"
        return ""


class User(AccountObject):
    _snowflake_type = "user"
    # Simplified user resource
    login_name: Optional[str] = Field(None)
    display_name: Optional[str] = Field(None)
    email: Optional[str] = Field(None)
    disabled: bool = Field(False)
    default_role: Optional[str] = Field(None)
    default_warehouse: Optional[str] = Field(None)
    comment: Optional[str] = Field(None)

    def get_create_sql(self) -> str:
        sql = f"CREATE USER {self.name}"
        if self.login_name:
            sql += f" LOGIN_NAME = '{self.login_name}'"
        if self.display_name:
            sql += f" DISPLAY_NAME = '{self.display_name}'"
        if self.email:
            sql += f" EMAIL = '{self.email}'"
        if self.disabled:
            sql += " DISABLED = TRUE"
        if self.default_role:
            sql += f" DEFAULT_ROLE = '{self.default_role}'"
        if self.default_warehouse:
            sql += f" DEFAULT_WAREHOUSE = '{self.default_warehouse}'"
        if self.comment:
            sql += f" COMMENT = '{self.comment}'"
        return sql

    def get_alter_sql(self, current_state: "User") -> str:
        changes = []
        if self.disabled != current_state.disabled:
            changes.append(f"DISABLED = {str(self.disabled).upper()}")
        # ... others ...
        if not changes:
            return ""
        return f"ALTER USER {self.name} SET " + ", ".join(changes)


class Grant(Resource):
    _snowflake_type = "grant"
    privilege: str = Field(..., description="USAGE, SELECT, etc.")
    on_type: str = Field(..., description="DATABASE, SCHEMA, TABLE, etc.")
    on_name: str = Field(..., description="Name of the object")
    to_role: str = Field(..., description="Role to grant to")

    @property
    def identifier(self) -> str:
        # Unique ID for a grant is tricky.
        # GRANT USAGE ON DATABASE DB1 TO ROLE R1
        return f"GRANT.{self.privilege}.{self.on_type}.{self.on_name}.TO.{self.to_role}".upper()

    def get_create_sql(self) -> str:
        return f"GRANT {self.privilege} ON {self.on_type} {self.on_name} TO ROLE {self.to_role}"

    def get_drop_sql(self) -> str:
        return f"REVOKE {self.privilege} ON {self.on_type} {self.on_name} FROM ROLE {self.to_role}"

    def get_alter_sql(self, current_state: "Resource") -> str:
        # Grants are usually immutable. Drop and recreate if different?
        # But ID includes all fields, so if anything changes, ID changes, so it's a destroy/create pair.
        return ""
