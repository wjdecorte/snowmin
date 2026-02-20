"""Tests for the `snowmin apply` CLI command."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from snowmin.cli import cli


@pytest.fixture
def stub_stack(tmp_path: Path) -> Path:
    """A minimal valid stack .py file."""
    p = tmp_path / "stack.py"
    p.write_text("# empty stack\n")
    return p


class TestApplyCommand:
    def test_apply_no_changes(self, runner: CliRunner, stub_stack: Path, mocker):
        """When there are no planned changes, 'No changes to apply.' is printed."""
        mocker.patch("snowmin.core.runner.Runner.plan", return_value=[])
        mocker.patch("snowmin.core.runner.Introspector")
        mocker.patch("snowmin.core.runner.ConnectionManager")

        result = runner.invoke(cli, ["apply", "--stack", str(stub_stack)])

        assert result.exit_code == 0
        assert "No changes to apply." in result.output

    def test_apply_confirmed(self, runner: CliRunner, stub_stack: Path, mocker):
        """When user confirms, runner.apply() is called with the SQL plan."""
        sql = ["CREATE WAREHOUSE SNOWMIN_WH;"]
        _ = mocker.patch("snowmin.core.runner.Runner.plan", return_value=sql)
        mock_apply = mocker.patch("snowmin.core.runner.Runner.apply")
        mocker.patch("snowmin.core.runner.Introspector")
        mocker.patch("snowmin.core.runner.ConnectionManager")

        # Simulate user typing 'y' at the confirmation prompt
        result = runner.invoke(cli, ["apply", "--stack", str(stub_stack)], input="y\n")

        assert result.exit_code == 0
        mock_apply.assert_called_once_with(sql)

    def test_apply_cancelled(self, runner: CliRunner, stub_stack: Path, mocker):
        """When user declines, runner.apply() is NOT called and 'cancelled' is shown."""
        sql = ["CREATE WAREHOUSE SNOWMIN_WH;"]
        mocker.patch("snowmin.core.runner.Runner.plan", return_value=sql)
        mock_apply = mocker.patch("snowmin.core.runner.Runner.apply")
        mocker.patch("snowmin.core.runner.Introspector")
        mocker.patch("snowmin.core.runner.ConnectionManager")

        result = runner.invoke(cli, ["apply", "--stack", str(stub_stack)], input="n\n")

        assert result.exit_code == 0
        assert "Apply cancelled." in result.output
        mock_apply.assert_not_called()

    def test_apply_stack_not_found(self, runner: CliRunner):
        """A non-existent stack path produces an error and non-zero exit."""
        result = runner.invoke(cli, ["apply", "--stack", "missing.py"])

        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "Error" in result.output

    def test_apply_custom_stack_path(self, runner: CliRunner, tmp_path: Path, mocker):
        """A custom --stack path is accepted regardless of filename."""
        custom = tmp_path / "my_infra.py"
        custom.write_text("# custom stack\n")
        mocker.patch("snowmin.core.runner.Runner.plan", return_value=[])
        mocker.patch("snowmin.core.runner.Introspector")
        mocker.patch("snowmin.core.runner.ConnectionManager")

        result = runner.invoke(cli, ["apply", "--stack", str(custom)])

        assert result.exit_code == 0

    def test_apply_short_flag(self, runner: CliRunner, stub_stack: Path, mocker):
        """-s is an accepted alias for --stack."""
        mocker.patch("snowmin.core.runner.Runner.plan", return_value=[])
        mocker.patch("snowmin.core.runner.Introspector")
        mocker.patch("snowmin.core.runner.ConnectionManager")

        result = runner.invoke(cli, ["apply", "-s", str(stub_stack)])

        assert result.exit_code == 0
