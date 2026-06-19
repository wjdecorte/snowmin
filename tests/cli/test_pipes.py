"""Tests for pipe operation commands."""

from __future__ import annotations

from snowmin.operations.pipes import list_pipes_command, pause_pipe_command


class FakeCursor:
    def __init__(self, description=None, rows=None):
        self.description = description or []
        self._rows = rows or []

    def fetchall(self):
        return self._rows

    def close(self):
        pass


PIPE_DESCRIPTION = [
    ("name",),
    ("database_name",),
    ("schema_name",),
]


def _ctx(mocker):
    ctx = mocker.Mock()
    ctx.obj = {"settings": mocker.Mock(), "cli_overrides": {}}
    return ctx


def _patch_config(mocker):
    mocker.patch(
        "snowmin.operations.pipes.get_merged_connection_config",
        return_value={"database": "RAP_DEV_ANALYTICS"},
    )


def test_list_pipes_queries_comma_separated_schemas(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.pipes.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW PIPES IN SCHEMA RAP_DEV_ANALYTICS.SILVER":
            return FakeCursor(
                description=PIPE_DESCRIPTION,
                rows=[("CUSTOMER_PIPE", "RAP_DEV_ANALYTICS", "SILVER")],
            )
        if query == "SHOW PIPES IN SCHEMA RAP_DEV_ANALYTICS.GOLD":
            return FakeCursor(
                description=PIPE_DESCRIPTION,
                rows=[("ORDER_PIPE", "RAP_DEV_ANALYTICS", "GOLD")],
            )
        if query.startswith("SELECT '"):
            return FakeCursor(rows=[("CUSTOMER_PIPE", '{"executionState":"RUNNING"}')])
        return FakeCursor()

    execute.side_effect = execute_side_effect

    list_pipes_command(_ctx(mocker), schema="SILVER,GOLD")

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert "SHOW PIPES IN SCHEMA RAP_DEV_ANALYTICS.SILVER" in executed_sql
    assert "SHOW PIPES IN SCHEMA RAP_DEV_ANALYTICS.GOLD" in executed_sql


def test_pause_pipe_applies_unqualified_name_to_comma_separated_schemas(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.pipes.ConnectionManager.execute")
    execute.return_value = FakeCursor()

    pause_pipe_command(
        _ctx(mocker),
        pipe_name="LOAD_PIPE",
        pattern=None,
        schema="SILVER,GOLD",
        status=None,
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert (
        "ALTER PIPE RAP_DEV_ANALYTICS.SILVER.LOAD_PIPE SET PIPE_EXECUTION_PAUSED = TRUE"
        in executed_sql
    )
    assert (
        "ALTER PIPE RAP_DEV_ANALYTICS.GOLD.LOAD_PIPE SET PIPE_EXECUTION_PAUSED = TRUE"
        in executed_sql
    )
