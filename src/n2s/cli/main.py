# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/n2s/cli/main.py

"""Main CLI entry point for n2s."""

import typer
from typing import Optional

from n2s.cli.commands.push import main as push_command
from n2s.cli.commands.pull import main as pull_command  
from n2s.cli.commands.status import main as status_command
from n2s.logging.setup import setup_logging

app = typer.Typer(
    name="n2s",
    help="Storage coordination service with path-aware blob creation and disaster recovery",
    no_args_is_help=True,
)

app.command("push", help="Store files in a new changeset")(push_command)
app.command("pull", help="Restore files from storage")(pull_command)
app.command("status", help="Query changeset and upload status")(status_command)


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Path to config file"),
):
    """n2s: Storage coordination service."""
    setup_logging(verbose=verbose)


if __name__ == "__main__":
    app()