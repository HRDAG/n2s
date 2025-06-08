# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/n2s/cli/commands/push.py

"""Push command for storing files."""

import typer
from typing import List, Optional
from pathlib import Path


def main(
    files: List[Path] = typer.Argument(..., help="Files to store"),
    changeset_name: str = typer.Option(..., "--changeset-name", "-n", help="Name for this changeset"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Config file path"),
):
    """Store files in a new changeset."""
    typer.echo(f"Would push {len(files)} files to changeset '{changeset_name}'")
    for file in files:
        typer.echo(f"  - {file}")
    typer.echo("(Not implemented yet)")