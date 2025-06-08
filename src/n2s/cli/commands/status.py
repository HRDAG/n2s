# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/n2s/cli/commands/status.py

"""Status command for querying changeset and upload status."""

import typer
from typing import Optional


def main(
    changeset_id: Optional[str] = typer.Option(None, "--changeset-id", "-i", help="Specific changeset to query"),
    backend: Optional[str] = typer.Option(None, "--backend", "-b", help="Backend to query"),
):
    """Query changeset and upload status."""
    if changeset_id:
        typer.echo(f"Would show status for changeset '{changeset_id}'")
    elif backend:
        typer.echo(f"Would show status for backend '{backend}'")
    else:
        typer.echo("Would show overall status")
    typer.echo("(Not implemented yet)")