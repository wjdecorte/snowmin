from pydantic import BaseModel, Field
from snowmin.resources.account import Warehouse, Role, User
from snowmin.resources.database import Database, Schema

# Account level
wh = Warehouse(name="SNOWMIN_TEST_WH", warehouse_size="X-SMALL")
role = Role(name="SNOWMIN_ROLE", comment="Managed role")
user = User(name="SNOWMIN_USER", login_name="flakeuser", default_role="SNOWMIN_ROLE")

# Database level
db = Database(name="SNOWMIN_DB", data_retention_time_in_days=1)
schema = Schema(database="SNOWMIN_DB", name="APP_SCHEMA", managed_access=True)


# Schema Objects (Table)
class Customers(BaseModel):
    id: int
    name: str = Field(max_length=100)
    active: bool = True
    score: float


schema.add_table(Customers)
