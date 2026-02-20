from typing import List, Type, Any, Optional
from pydantic import BaseModel, Field
from snowmin.core.state import Resource


class Column(BaseModel):
    name: str
    type: str
    nullable: bool = True
    primary_key: bool = False
    comment: Optional[str] = None


class Table(Resource):
    _snowflake_type = "table"

    database: str
    schema_name: str = Field(..., alias="schema")
    columns: List[Column]
    comment: Optional[str] = None

    @property
    def identifier(self) -> str:
        return f"{self._snowflake_type}.{self.database.upper()}.{self.schema_name.upper()}.{self.name.upper()}"

    @classmethod
    def from_model(cls, database: str, schema: str, model: Type[BaseModel]) -> "Table":
        """Create a Table resource from a Pydantic model."""
        columns = []

        for name, field_info in model.model_fields.items():
            col_type = cls._map_type(field_info.annotation, field_info)

            # Simple assumption for nullability/pk for MVP
            columns.append(
                Column(
                    name=name.upper(),
                    type=col_type,
                    nullable=True,
                    primary_key=False,
                    comment=str(field_info.description)
                    if field_info.description
                    else None,
                )
            )

        return cls(
            name=model.__name__, database=database, schema=schema, columns=columns
        )

    @staticmethod
    def _map_type(py_type: Any, field_info: Any) -> str:
        if py_type is int:
            return "NUMBER"
        if py_type is str:
            if field_info.metadata:
                for meta in field_info.metadata:
                    if hasattr(meta, "max_length") and meta.max_length:
                        return f"VARCHAR({meta.max_length})"
            return "VARCHAR"
        if py_type is bool:
            return "BOOLEAN"
        if py_type is float:
            return "FLOAT"
        return "VARCHAR"

    def get_create_sql(self) -> str:
        full_name = f"{self.database}.{self.schema_name}.{self.name}"
        cols_sql = []
        for col in self.columns:
            line = f"{col.name} {col.type}"
            if not col.nullable:
                line += " NOT NULL"
            if col.comment:
                line += f" COMMENT '{col.comment}'"
            cols_sql.append(line)

        return f"CREATE TABLE {full_name} ({', '.join(cols_sql)})"

    def get_alter_sql(self, current_state: "Table") -> str:
        full_name = f"{self.database}.{self.schema_name}.{self.name}"
        sql_stmts = []

        desired_cols = {c.name: c for c in self.columns}
        current_cols = {c.name: c for c in current_state.columns}

        for name, col in desired_cols.items():
            if name not in current_cols:
                line = f"{col.name} {col.type}"
                if not col.nullable:
                    line += " NOT NULL"
                sql_stmts.append(f"ADD COLUMN {line}")

        if sql_stmts:
            return f"ALTER TABLE {full_name} " + ", ".join(sql_stmts)

        return ""
