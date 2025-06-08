# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/n2s/cli/commands/pull.py

"""Pull command for restoring files."""

import typer
from typing import Optional, List
from pathlib import Path


def main(
    changeset_id: str = typer.Option(..., "--changeset-id", "-i", help="Changeset to restore"),
    target_path: Path = typer.Option(..., "--target-path", "-t", help="Where to restore files"),
    files: Optional[List[str]] = typer.Option(None, "--files", "-f", help="Specific files to restore"),
    force: bool = typer.Option(False, "--force", help="Overwrite existing files"),
):
    """Restore files from storage."""
    typer.echo(f"Would restore changeset '{changeset_id}' to '{target_path}'")
    if files:
        typer.echo(f"  Specific files: {files}")
    if force:
        typer.echo("  With force overwrite")
    typer.echo("(Not implemented yet)")