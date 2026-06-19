"""Tests for Snowflake connection argument handling."""

from __future__ import annotations

from snowmin.core.connection import ConnectionManager


def test_connection_does_not_set_session_schema_for_schema_list(mocker):
    connect = mocker.patch("snowmin.core.connection.snowflake.connector.connect")
    connect.return_value = mocker.Mock()
    ConnectionManager.close()

    try:
        ConnectionManager.get_connection(
            {
                "account": "acct",
                "user": "user",
                "database": "RAP_DEV_ANALYTICS",
                "schema": "SILVER,GOLD",
                "password": "secret",
            }
        )
    finally:
        ConnectionManager.close()

    conn_args = connect.call_args.kwargs
    assert conn_args["database"] == "RAP_DEV_ANALYTICS"
    assert "schema" not in conn_args
