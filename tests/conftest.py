"""Shared pytest fixtures for snowmin tests."""

from __future__ import annotations

import pytest
from click.testing import CliRunner


@pytest.fixture
def runner():
    """Return a Click CliRunner for testing CLI commands."""
    return CliRunner()
