# Snowmin

Snowmin is a Python CLI for managing Snowflake infrastructure from code. Define
warehouses, roles, users, databases, schemas, and tables in a Python stack file,
then use `snowmin plan` to preview SQL changes and `snowmin apply` to execute
them.

The project also includes operational commands for common Snowflake objects such
as tasks, streams, tables, and pipes.

## Status

Snowmin is early-stage software. The current implementation can load Python
stack files, introspect supported Snowflake objects, generate create/alter/drop
SQL, and apply planned SQL after confirmation. Some commands are placeholders:
`destroy` and `import` are not implemented yet.

## Requirements

- Python 3.14 or newer
- `uv`
- A Snowflake account and connection configuration

## Installation

From the repository root:

```bash
uv sync
uv run snowmin --help
```

For local development:

```bash
uv run pytest
```

## Configuration

Snowmin reads configuration from multiple sources. Later items are lower
priority:

1. CLI flags such as `--database`, `--schema`, `--warehouse`, and `--role`
2. Environment variables prefixed with `SNOWMIN__`
3. An environment-specific file named `snowmin_{env}.yaml` in the current
   directory, where `{env}` comes from `SNOWMIN_ENV` and defaults to `dev`
4. `~/.snowmin/config.yaml`
5. A Snowflake connection profile in `~/.snowflake/connections.toml`

Create or update persistent Snowmin settings with:

```bash
uv run snowmin config set connection default
uv run snowmin config set database MY_DATABASE
uv run snowmin config set schema MY_SCHEMA
uv run snowmin config show
```

Snowmin can also use Snowflake's `connections.toml` format:

```toml
[default]
account = "my_account"
user = "my_user"
role = "SYSADMIN"
warehouse = "MY_WH"
database = "MY_DATABASE"
schema = "PUBLIC"
```

Authentication supports password, private key, or external browser auth. If no
password or private key is configured, Snowmin falls back to Snowflake external
browser authentication.

## Defining a Stack

By default, Snowmin looks for `stack.py` in the current directory. A stack is
normal Python code that instantiates Snowmin resource objects.

```python
from pydantic import BaseModel, Field

from snowmin.resources.account import Role, User, Warehouse
from snowmin.resources.database import Database, Schema


warehouse = Warehouse(name="SNOWMIN_TEST_WH", warehouse_size="X-SMALL")
role = Role(name="SNOWMIN_ROLE", comment="Managed role")
user = User(
    name="SNOWMIN_USER",
    login_name="flakeuser",
    default_role="SNOWMIN_ROLE",
)

database = Database(name="SNOWMIN_DB", data_retention_time_in_days=1)
schema = Schema(database="SNOWMIN_DB", name="APP_SCHEMA", managed_access=True)


class Customers(BaseModel):
    id: int
    name: str = Field(max_length=100)
    active: bool = True
    score: float


schema.add_table(Customers)
```

`Schema.add_table()` maps a Pydantic model to a Snowflake table. Current type
mapping includes:

- `int` -> `NUMBER`
- `str` -> `VARCHAR` or `VARCHAR(n)` when `max_length` is set
- `bool` -> `BOOLEAN`
- `float` -> `FLOAT`

## Planning and Applying

Preview changes:

```bash
uv run snowmin plan
```

Use a custom stack file:

```bash
uv run snowmin plan --stack path/to/infra.py
uv run snowmin plan -s path/to/infra.py
```

Apply changes after reviewing the plan:

```bash
uv run snowmin apply
```

`apply` asks for confirmation before executing SQL.

## Object Management

In addition to declarative stack management, Snowmin includes direct operational
commands for managing existing Snowflake tasks, streams, pipes, and tables.
These commands use the same connection configuration and global overrides as
`plan` and `apply`.

Most commands accept either the schema from your active configuration or a
`--schema` option. Schema values can be passed as either `SCHEMA` or
`DATABASE.SCHEMA`.

```bash
uv run snowmin --connection prod --database ANALYTICS --schema MART streams list
uv run snowmin tasks list --schema ANALYTICS.MART
```

### Tasks

Task commands use `SHOW TASKS` to discover tasks and `ALTER TASK` to suspend or
resume them.

```bash
uv run snowmin tasks list
uv run snowmin tasks list --schema ANALYTICS.MART
uv run snowmin tasks list --pattern '^LOAD_' --status started
```

Suspend or resume one task:

```bash
uv run snowmin tasks suspend LOAD_CUSTOMERS
uv run snowmin tasks resume LOAD_CUSTOMERS
```

Suspend or resume multiple tasks with confirmation:

```bash
uv run snowmin tasks suspend --all --schema ANALYTICS.MART
uv run snowmin tasks resume --pattern '^LOAD_' --schema ANALYTICS.MART
```

Bulk task operations print the matched tasks and ask for confirmation before
running `ALTER TASK ... SUSPEND` or `ALTER TASK ... RESUME`. Suspend skips tasks
that are not started, and resume skips tasks that are not suspended.

### Streams

Stream commands can list streams, show whether streams have pending change data,
create streams on tables, drop streams, or reset streams by dropping and
recreating them from their current DDL.

List streams:

```bash
uv run snowmin streams list
uv run snowmin streams list --schema ANALYTICS.MART
uv run snowmin streams list --pattern '_STREAM$'
uv run snowmin streams list --has-data
uv run snowmin streams list --no-data
```

`streams list` uses `SHOW STREAMS` and checks each stream with
`SYSTEM$STREAM_HAS_DATA`. Output includes stream mode, source table, stale
status, and data availability.

Create a stream on a table:

```bash
uv run snowmin streams create CUSTOMER_STREAM CUSTOMERS --schema ANALYTICS.MART
uv run snowmin streams create CUSTOMER_STREAM CUSTOMERS --mode APPEND_ONLY
uv run snowmin streams create CUSTOMER_STREAM CUSTOMERS --at '2026-01-01 00:00:00'
uv run snowmin streams create CUSTOMER_STREAM CUSTOMERS --before '2026-01-01 00:00:00'
```

Supported stream modes are `DEFAULT`, `APPEND_ONLY`, and `INSERT_ONLY`. When a
database and schema are available, stream creation switches to an owner role
derived from the naming convention `ROLE_<env>_<schema>_OWNER`, where `<env>` is
the second underscore-separated part of the database name.

Drop or reset a stream:

```bash
uv run snowmin streams drop CUSTOMER_STREAM --schema ANALYTICS.MART
uv run snowmin streams reset CUSTOMER_STREAM --schema ANALYTICS.MART
uv run snowmin streams reset --all --schema ANALYTICS.MART
uv run snowmin streams reset CUSTOMER_STREAM --at '2026-01-01 00:00:00'
```

`streams drop` asks for confirmation. `streams reset` fetches the stream DDL with
`GET_DDL`, preserves the existing comment when possible, optionally adds an
`AT (TIMESTAMP => ...)` clause, then drops and recreates the stream after
confirmation. Use `--all` to reset every stream in the selected schema.

### Pipes

Pipe commands use `SHOW PIPES` for discovery and `SYSTEM$PIPE_STATUS` for
detailed execution status. Status filters are case-insensitive and commonly use
values such as `RUNNING`, `PAUSED`, or `STALLED`.

List pipes:

```bash
uv run snowmin pipes list
uv run snowmin pipes list --schema ANALYTICS.MART
uv run snowmin pipes list --pattern '^LOAD_'
uv run snowmin pipes list --status RUNNING
```

Refresh, pause, or resume one pipe:

```bash
uv run snowmin pipes refresh LOAD_CUSTOMERS_PIPE
uv run snowmin pipes pause LOAD_CUSTOMERS_PIPE
uv run snowmin pipes resume LOAD_CUSTOMERS_PIPE
```

Process multiple pipes by regex pattern, optionally filtered by current status:

```bash
uv run snowmin pipes refresh --pattern '^LOAD_' --status RUNNING
uv run snowmin pipes pause --pattern '^LOAD_' --schema ANALYTICS.MART
uv run snowmin pipes resume --pattern '^LOAD_' --status PAUSED
```

Pattern-based pipe operations print the matched pipes and ask for confirmation
before making changes. The generated SQL is:

- `ALTER PIPE ... REFRESH`
- `ALTER PIPE ... SET PIPE_EXECUTION_PAUSED = TRUE`
- `ALTER PIPE ... SET PIPE_EXECUTION_PAUSED = FALSE`

Drop and recreate pipes from their current DDL:

```bash
uv run snowmin pipes drop-recreate LOAD_CUSTOMERS_PIPE
uv run snowmin pipes drop-recreate --pattern '^LOAD_' --schema ANALYTICS.MART
uv run snowmin pipes drop-recreate --all --schema ANALYTICS.MART
uv run snowmin pipes drop-recreate --all --skip-status
```

`pipes drop-recreate` fetches each pipe's DDL with `GET_DDL`, drops the pipe, sets
the schema context when possible, and recreates the pipe. It asks for
confirmation before destructive work and aborts on the first pipe recreation
error.

### Tables

Table commands currently include a guarded truncate operation:

```bash
uv run snowmin tables truncate ANALYTICS.MART.CUSTOMERS
```

`tables truncate` asks for confirmation and then runs
`TRUNCATE TABLE <table_name>`.

## CLI Overview

```bash
uv run snowmin --help
```

Top-level commands:

- `config`: show or update Snowmin configuration
- `plan`: show SQL required to reach the desired stack state
- `apply`: execute planned SQL after confirmation
- `tasks`: list, suspend, and resume Snowflake tasks
- `tables`: truncate Snowflake tables
- `streams`: list, create, drop, and reset Snowflake streams
- `pipes`: list, refresh, pause, resume, and drop/recreate Snowflake pipes
- `destroy`: placeholder
- `import`: placeholder

Examples:

```bash
uv run snowmin tasks list --schema MY_SCHEMA
uv run snowmin tasks suspend MY_TASK
uv run snowmin streams list --has-data
uv run snowmin streams create MY_STREAM MY_TABLE --schema MY_SCHEMA
uv run snowmin pipes refresh MY_PIPE
uv run snowmin tables truncate MY_TABLE
```

Global connection overrides can be passed before the command:

```bash
uv run snowmin --connection prod --database ANALYTICS --schema MART plan
```

## Development

Run the test suite:

```bash
uv run pytest
```

Useful source locations:

- `src/snowmin/cli.py`: Click command definitions
- `src/snowmin/core/config.py`: configuration loading and precedence
- `src/snowmin/core/stack_loader.py`: stack file loading
- `src/snowmin/core/runner.py`: plan/apply orchestration
- `src/snowmin/resources/`: declarative resource models
- `src/snowmin/operations/`: operational commands for tasks, streams, tables,
  and pipes

## License

This project is licensed under the terms in [LICENSE](LICENSE).
