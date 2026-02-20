"""Dynamically loads a user-supplied stack module from a file path."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import click


def load_stack(path: str) -> ModuleType:
    """Load a stack Python file from *path* and execute it.

    The module is registered in ``sys.modules`` under the key ``"stack"``
    so that any relative imports inside the stack file resolve correctly.

    Args:
        path: Absolute or CWD-relative path to a ``.py`` stack file.

    Returns:
        The loaded module.

    Raises:
        click.ClickException: If the file is not found or is not a ``.py`` file.
    """
    stack_path = Path(path).resolve()

    if not stack_path.exists():
        raise click.ClickException(f"Stack file not found: '{path}'")

    if stack_path.suffix != ".py":
        raise click.ClickException(
            f"Stack file must be a Python (.py) file, got: '{stack_path.name}'"
        )

    # Add the directory containing the stack file to sys.path so that
    # relative imports inside the stack (e.g. from my_helpers import ...) work.
    stack_dir = str(stack_path.parent)
    if stack_dir not in sys.path:
        sys.path.insert(0, stack_dir)

    spec = importlib.util.spec_from_file_location("stack", stack_path)
    if spec is None or spec.loader is None:
        raise click.ClickException(f"Could not load stack file: '{path}'")

    module = importlib.util.module_from_spec(spec)
    sys.modules["stack"] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]

    return module
