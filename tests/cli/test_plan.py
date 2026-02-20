"""Tests for the `snowmin plan` CLI command."""

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


class TestPlanCommand:
    def test_plan_no_changes(self, runner: CliRunner, stub_stack: Path, mocker):
        """When Runner.plan() returns [], 'No changes detected.' is printed."""
        mocker.patch("snowmin.core.runner.Runner.plan", return_value=[])
        mocker.patch("snowmin.core.runner.Introspector")
        mocker.patch("snowmin.core.runner.ConnectionManager")

        result = runner.invoke(cli, ["plan", "--stack", str(stub_stack)])

        assert result.exit_code == 0
        assert "No changes detected." in result.output

    def test_plan_with_changes(self, runner: CliRunner, stub_stack: Path, mocker):
        """When Runner.plan() returns SQL, the SQL lines are printed."""
        sql = ["CREATE WAREHOUSE SNOWMIN_WH;"]
        mocker.patch("snowmin.core.runner.Runner.plan", return_value=sql)
        mocker.patch("snowmin.core.runner.Introspector")
        mocker.patch("snowmin.core.runner.ConnectionManager")

        result = runner.invoke(cli, ["plan", "--stack", str(stub_stack)])

        assert result.exit_code == 0
        assert "Proposed Changes:" in result.output
        assert sql[0] in result.output

    def test_plan_stack_not_found(self, runner: CliRunner):
        """A non-existent stack path produces an error and non-zero exit."""
        result = runner.invoke(cli, ["plan", "--stack", "missing.py"])

        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "Error" in result.output

    def test_plan_custom_stack_path(self, runner: CliRunner, tmp_path: Path, mocker):
        """A custom --stack path is loaded regardless of filename."""
        custom = tmp_path / "my_infra.py"
        custom.write_text("# custom stack\n")
        mocker.patch("snowmin.core.runner.Runner.plan", return_value=[])
        mocker.patch("snowmin.core.runner.Introspector")
        mocker.patch("snowmin.core.runner.ConnectionManager")

        result = runner.invoke(cli, ["plan", "--stack", str(custom)])

        assert result.exit_code == 0

    def test_plan_short_flag(self, runner: CliRunner, stub_stack: Path, mocker):
        """-s is an accepted alias for --stack."""
        mocker.patch("snowmin.core.runner.Runner.plan", return_value=[])
        mocker.patch("snowmin.core.runner.Introspector")
        mocker.patch("snowmin.core.runner.ConnectionManager")

        result = runner.invoke(cli, ["plan", "-s", str(stub_stack)])

        assert result.exit_code == 0
