"""Tests for stream CLI commands."""

from __future__ import annotations

from click.testing import CliRunner

from snowmin.cli import cli
from snowmin.operations.streams import reset_stream_command


class FakeCursor:
    def __init__(self, description=None, rows=None, row=None):
        self.description = description or []
        self._rows = rows or []
        self._row = row

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._row

    def close(self):
        pass


def test_streams_reset_accepts_all_option(runner: CliRunner, mocker):
    mock_reset = mocker.patch("snowmin.operations.streams.reset_stream_command")

    result = runner.invoke(
        cli,
        [
            "streams",
            "reset",
            "--all",
            "--schema",
            "ANALYTICS.MART",
            "--at",
            "2026-01-01 00:00:00",
        ],
    )

    assert result.exit_code == 0
    _, stream_name, all_streams, schema, at = mock_reset.call_args.args
    assert stream_name is None
    assert all_streams is True
    assert schema == "ANALYTICS.MART"
    assert at == "2026-01-01 00:00:00"


def test_reset_stream_all_discovers_and_resets_streams(mocker):
    ctx = mocker.Mock()
    ctx.obj = {"settings": mocker.Mock(), "cli_overrides": {}}
    execute = mocker.patch("snowmin.operations.streams.ConnectionManager.execute")
    mocker.patch(
        "snowmin.operations.streams.get_merged_connection_config",
        return_value={"database": "RAP_DEV_ANALYTICS"},
    )
    mocker.patch("snowmin.operations.streams.click.confirm", return_value=True)

    def execute_side_effect(query, conn_config):
        if query == "SHOW STREAMS IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=[
                    ("name",),
                    ("database_name",),
                    ("schema_name",),
                ],
                rows=[
                    ("CUSTOMER_STREAM", "RAP_DEV_ANALYTICS", "MART"),
                    ("ORDER_STREAM", "RAP_DEV_ANALYTICS", "MART"),
                ],
            )
        if query.startswith("SELECT GET_DDL('stream', '"):
            stream_name = query.rsplit(".", 1)[-1].rstrip("')")
            return FakeCursor(
                row=(
                    f"CREATE STREAM {stream_name} "
                    f"ON TABLE RAP_DEV_ANALYTICS.MART.{stream_name}_SOURCE",
                )
            )
        if query.startswith("SHOW STREAMS LIKE"):
            return FakeCursor(description=[("comment",)], rows=[("kept",)])
        return FakeCursor()

    execute.side_effect = execute_side_effect

    reset_stream_command(
        ctx,
        stream_name=None,
        all_flag=True,
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert "DROP STREAM RAP_DEV_ANALYTICS.MART.CUSTOMER_STREAM" in executed_sql
    assert "DROP STREAM RAP_DEV_ANALYTICS.MART.ORDER_STREAM" in executed_sql
    assert executed_sql.count("USE SCHEMA RAP_DEV_ANALYTICS.MART") == 2
    assert (
        "ALTER STREAM RAP_DEV_ANALYTICS.MART.CUSTOMER_STREAM SET COMMENT = 'kept'"
        in executed_sql
    )
    assert (
        "ALTER STREAM RAP_DEV_ANALYTICS.MART.ORDER_STREAM SET COMMENT = 'kept'"
        in executed_sql
    )


def test_reset_stream_all_queries_comma_separated_schemas(mocker):
    ctx = mocker.Mock()
    ctx.obj = {"settings": mocker.Mock(), "cli_overrides": {}}
    execute = mocker.patch("snowmin.operations.streams.ConnectionManager.execute")
    mocker.patch(
        "snowmin.operations.streams.get_merged_connection_config",
        return_value={"database": "RAP_DEV_ANALYTICS"},
    )
    mocker.patch("snowmin.operations.streams.click.confirm", return_value=True)

    def execute_side_effect(query, conn_config):
        if query == "SHOW STREAMS IN SCHEMA RAP_DEV_ANALYTICS.SILVER":
            return FakeCursor(
                description=[("name",), ("database_name",), ("schema_name",)],
                rows=[("CUSTOMER_STREAM", "RAP_DEV_ANALYTICS", "SILVER")],
            )
        if query == "SHOW STREAMS IN SCHEMA RAP_DEV_ANALYTICS.GOLD":
            return FakeCursor(
                description=[("name",), ("database_name",), ("schema_name",)],
                rows=[("ORDER_STREAM", "RAP_DEV_ANALYTICS", "GOLD")],
            )
        if query.startswith("SELECT GET_DDL('stream', '"):
            stream_name = query.rsplit(".", 1)[-1].rstrip("')")
            return FakeCursor(
                row=(
                    f"CREATE STREAM {stream_name} "
                    f"ON TABLE RAP_DEV_ANALYTICS.SILVER.{stream_name}_SOURCE",
                )
            )
        if query.startswith("SHOW STREAMS LIKE"):
            return FakeCursor(description=[("comment",)], rows=[])
        return FakeCursor()

    execute.side_effect = execute_side_effect

    reset_stream_command(
        ctx,
        stream_name=None,
        all_flag=True,
        schema="SILVER,GOLD",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert "SHOW STREAMS IN SCHEMA RAP_DEV_ANALYTICS.SILVER" in executed_sql
    assert "SHOW STREAMS IN SCHEMA RAP_DEV_ANALYTICS.GOLD" in executed_sql
    assert "DROP STREAM RAP_DEV_ANALYTICS.SILVER.CUSTOMER_STREAM" in executed_sql
    assert "DROP STREAM RAP_DEV_ANALYTICS.GOLD.ORDER_STREAM" in executed_sql
    assert "USE ROLE ROLE_DEV_SILVER_OWNER" in executed_sql
    assert "USE ROLE ROLE_DEV_GOLD_OWNER" in executed_sql
