"""Tests for snowmin.core.stack_loader.load_stack()."""

from __future__ import annotations

from pathlib import Path

import pytest
import click

from snowmin.core.stack_loader import load_stack


class TestLoadStack:
    """Unit tests for load_stack()."""

    def test_load_stack_success(self, tmp_path: Path):
        """A valid .py file loads without error and module is returned."""
        stack_file = tmp_path / "my_stack.py"
        stack_file.write_text("x = 42\n")

        module = load_stack(str(stack_file))

        assert module is not None
        assert module.x == 42

    def test_load_stack_file_not_found(self, tmp_path: Path):
        """Non-existent path raises ClickException."""
        missing = tmp_path / "does_not_exist.py"

        with pytest.raises(click.ClickException, match="not found"):
            load_stack(str(missing))

    def test_load_stack_not_a_python_file(self, tmp_path: Path):
        """Non-.py extension raises ClickException."""
        bad_file = tmp_path / "config.yaml"
        bad_file.write_text("key: value\n")

        with pytest.raises(click.ClickException, match=r"\.py"):
            load_stack(str(bad_file))

    def test_load_stack_relative_path(self, tmp_path: Path, monkeypatch):
        """Relative path resolved from CWD."""
        stack_file = tmp_path / "stack.py"
        stack_file.write_text("value = 'hello'\n")
        monkeypatch.chdir(tmp_path)

        module = load_stack("stack.py")

        assert module.value == "hello"

    def test_load_stack_adds_parent_to_syspath(self, tmp_path: Path):
        """The stack file's parent directory is added to sys.path."""
        import sys

        stack_file = tmp_path / "stack.py"
        stack_file.write_text("")

        load_stack(str(stack_file))

        assert str(tmp_path) in sys.path
