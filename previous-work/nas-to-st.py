# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# resource-utils/bin/nas-to-st.py

from __future__ import annotations

import shutil
import subprocess
import tempfile
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Final, Iterator, Literal
from zoneinfo import ZoneInfo

import typer
from loguru import logger
from pydantic import BaseModel
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    before_log,
    after_log,
    retry_if_exception_type,
)

from hrdaglib import acquire_lock, handle_exit, release_lock, setup_logging


# ASSUMPTION: This script assumes that regular ZFS snapshots are being taken
# on all datasets (backup, working, legacy) by an external process.

# Constants
LOG_BASE_PATH: Final = Path("/var/log/nas-to-st")
STATE_BASE_PATH: Final = Path("/var/lib/nas-to-st")


FIRST_RUN_MODE = MappingProxyType({
    "backup": "diff_from_explicit",
    "working": "copy_all",
    "legacy": "baseline_only",
    "test": "copy_all"
})


class ZfsAction(str, Enum):
    MODIFIED = "M"
    ADDED = "+"
    REMOVED = "-"
    OTHER = "?"


class RcloneError(Exception):
    """Raised when rclone command fails."""
    def __init__(self, returncode: int, batch_idx: int, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.batch_idx = batch_idx
        self.stdout = stdout
        self.stderr = stderr
        msg = f"Rclone batch {batch_idx + 1} failed with return code {returncode}"
        if stderr:
            msg += f"\nSTDERR: {stderr[:500]}"  # Truncate to avoid huge messages
        # TODO: Parse JSON output from rclone if --json flag is used
        super().__init__(msg)


class SyncConfig(BaseModel):
    name: str
    dataset: str
    mountpoint: Path
    remote: str
    control_rclone_conf_path: Path
    rclone_opts: list[str] = [
        "--bwlimit", "50M",
        "--transfers", "8",
        "--checkers", "16",
        "--ignore-times", "--size-only",
        "--low-level-retries=10",
        "--retries=3",
        "--retries-sleep", "30s",
        "--tpslimit=5",
        "--multi-thread-streams=1",
        "--multi-thread-cutoff=512M",
        "--use-mmap",
        "--s3-upload-cutoff", "128M",
        "--s3-chunk-size", "32M",
        "--max-transfer", "3T",
        "--disable", "ListR",
        "--skip-links",
        "--stats", "60s",
        "--stats-one-line",
        "--stats-log-level", "NOTICE",
        "--progress",
    ]
    ssh_host: str
    remote_rclone_conf_path: Path = Path()
    bootstrap_snapshot: str | None = None  # For diff_from_explicit mode

    def model_post_init(self, __context):
        # TODO: validate that control_rclone_conf_path exists before attempting to copy
        tmpname = f"/tmp/rclone_{uuid.uuid4().hex}.conf"
        self.remote_rclone_conf_path = Path(tmpname)
        try:
            # TODO: why are we keeping the local copy? what's going on with shutil.copy?
            shutil.copy(self.control_rclone_conf_path, self.remote_rclone_conf_path)
            subprocess.run([
                "scp", str(self.control_rclone_conf_path),
                f"{self.ssh_host}:{self.remote_rclone_conf_path}"
            ], check=True)
        except Exception as e:
            raise RuntimeError(f"Failed to copy rclone config to NAS: {e}")



class ZfsChange(BaseModel):
    action: ZfsAction
    path: Path

    @classmethod
    def from_parts(cls, action: str, path: str) -> ZfsChange:
        try:
            return cls(action=ZfsAction(action), path=Path(path))
        except ValueError:
            return cls(action=ZfsAction.OTHER, path=Path(path))
        # unreachable code after return removed

    def relative_to_mountpoint(self, mountpoint: Path) -> str | None:
        try:
            return str(self.path.relative_to(mountpoint))
        except ValueError:
            return None


def _zfs_diff(cfg: SyncConfig, previous: str, current: str) -> list[ZfsChange]:
    cmd = [
        "zfs", "diff", "-FH", previous, current
    ]
    result = run_ssh(cfg.ssh_host, cmd, capture_output=True)
    lines = result.stdout.strip().splitlines()
    changes = []
    for line in lines:
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        action, path = parts[0], parts[-1]
        changes.append(ZfsChange.from_parts(action, path))
    return changes


def _write_files_from_list(
        cfg: SyncConfig, changes: list[ZfsChange],
        direction: Literal["copy", "delete"]) -> Path:
    target = STATE_BASE_PATH / f"{cfg.name}_{direction}_list.txt"
    if direction == "copy":
        filtered = [
            c.relative_to_mountpoint(cfg.mountpoint)
            for c in changes
            if c.action in {ZfsAction.MODIFIED, ZfsAction.ADDED}
        ]
    else:
        filtered = [
            c.relative_to_mountpoint(cfg.mountpoint)
            for c in changes
            if c.action == ZfsAction.REMOVED
        ]
    filtered = [p for p in filtered if p is not None]
    with target.open("w") as f:
        f.writelines(f"{p}\n" for p in filtered)
    return target


def _get_verified_snapshot(cfg: SyncConfig) -> str | None:
    log_path = LOG_BASE_PATH / f"{cfg.name}.log"
    if not log_path.exists():
        return None
    with log_path.open() as f:
        for line in reversed(f.readlines()):
            if "##Verified snapshot##:" in line:
                return line.split("##Verified snapshot##:", 1)[1].strip()
    return None


def _first_run_behavior(cfg: SyncConfig, mode: str) -> str | None:
    current = _get_latest_snapshot(cfg)

    if mode == "copy_all":
        # For working: sync everything from the current snapshot
        # TODO: there are millions of files, maybe 10-15M,
        # so what are we going to do with chunking? 15K chunk files?
        logger.info(f"No previous snapshot — copying everything from {current}")
        return None

    elif mode == "baseline_only":
        # For legacy: just record current snapshot and exit
        logger.success(f"##Verified snapshot##: {current}")
        logger.info("See you tomorrow!")
        return None

    elif mode == "diff_from_explicit":
        # For backup: use configured bootstrap snapshot
        if not cfg.bootstrap_snapshot:
            raise ValueError("bootstrap_snapshot must be set for diff_from_explicit mode")
        logger.info(f"Using configured bootstrap snapshot: {cfg.bootstrap_snapshot}")
        # TODO: validate that bootstrap snapshot exists
        return cfg.bootstrap_snapshot

    raise ValueError(f"Unknown first run mode: {mode}")


def run_ssh(
        host: str,
        cmd: list[str],
        capture_output: bool = False) -> subprocess.CompletedProcess:
    full_cmd = ["ssh", host] + cmd
    if capture_output:
        return subprocess.run(
            full_cmd, text=True, capture_output=True, check=True)
    else:
        return subprocess.run(full_cmd, check=True)


def sync_path(
        cfg: SyncConfig, dry_run: bool = False,
        max_retries: int = 1, chunk_dir: Path | None = None) -> None:
    # TODO: can we abstract Literal["copy", "delete"] as DIRECTIONS?
    def _rclone_batch_sync(files_from: Path, direction: Literal["copy", "delete"]) -> None:

        def _yield_files_from_chunks(source: Path, chunk_size: int = 10_000) -> Iterator[Path]:

            def _write_chunk(idx: int, chunk: list[str]) -> Path:
                path = STATE_BASE_PATH / f"rclone_{direction}_{cfg.name}_{idx:03d}.txt"
                with path.open("w") as f:
                    f.writelines(f"{line}\n" for line in chunk)
                return path

            chunk: list[str] = []
            chunk_idx = 0

            with source.open("r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    chunk.append(line)
                    if len(chunk) == chunk_size:
                        yield _write_chunk(chunk_idx, chunk)
                        chunk = []
                        chunk_idx += 1
                if chunk:
                    yield _write_chunk(chunk_idx, chunk)

        base_cmd = [
            "rclone", direction,
            "--config", str(cfg.remote_rclone_conf_path),
        ]

        if direction == "copy":
            base_cmd.extend([str(cfg.mountpoint), cfg.remote])
        else:  # delete
            base_cmd.extend([cfg.remote, "--rmdirs"])

        base_cmd.append("--no-traverse")

        if dry_run:
            base_cmd.append("--dry-run")
        base_cmd += cfg.rclone_opts

        @retry(
            stop=stop_after_attempt(max_retries + 1),
            wait=wait_exponential(multiplier=1, min=4, max=60),
            retry=retry_if_exception_type(RcloneError),
            before=before_log(logger, "INFO"),
            after=after_log(logger, "WARNING"),
        )
        def _run_rclone_batch(cmd: list[str], batch_idx: int, chunk_path: Path) -> None:
            """Run rclone command with retry logic."""
            logger.info(f"Running rclone {direction} batch {batch_idx + 1}")

            # Copy chunk file to nas
            remote_chunk_path = Path(f"/tmp/rclone_{cfg.name}_{direction}_{batch_idx:03d}_{uuid.uuid4().hex[:8]}.txt")
            try:
                subprocess.run([
                    "scp", str(chunk_path),
                    f"{cfg.ssh_host}:{remote_chunk_path}"
                ], check=True, capture_output=True, text=True)

                # Update command to use remote chunk path
                remote_cmd = cmd.copy()
                chunk_idx = cmd.index("--files-from")
                remote_cmd[chunk_idx + 1] = str(remote_chunk_path)

                # Run rclone on nas via SSH
                result = subprocess.run(
                    ["ssh", cfg.ssh_host] + remote_cmd,
                    capture_output=True,
                    text=True,
                    timeout=3600  # 1 hour timeout per batch
                )

                if result.returncode != 0:
                    raise RcloneError(
                        returncode=result.returncode,
                        batch_idx=batch_idx,
                        stdout=result.stdout,
                        stderr=result.stderr
                    )
                logger.success(f"{direction.title()} batch {batch_idx + 1} complete")

            finally:
                # Clean up remote chunk file
                subprocess.run(
                    ["ssh", cfg.ssh_host, "rm", "-f", str(remote_chunk_path)],
                    capture_output=True
                )

        failed_chunks: list[tuple[int, Path, str]] = []

        for idx, chunk_path in enumerate(_yield_files_from_chunks(files_from)):
            cmd = base_cmd + ["--files-from", str(chunk_path)]
            try:
                _run_rclone_batch(cmd, idx, chunk_path)
                chunk_path.unlink(missing_ok=True)
                logger.debug(f"Deleted temporary chunk file: {chunk_path}")
            except subprocess.TimeoutExpired:
                logger.error(f"Batch {idx + 1} timed out after 1 hour")
                logger.error(f"Failed chunk file saved at: {chunk_path}")
                failed_chunks.append((idx, chunk_path, "timeout"))
            except RcloneError as e:
                logger.error(f"Batch {idx + 1} failed after {max_retries + 1} attempts.")
                logger.error(f"Failed chunk file saved at: {chunk_path}")
                if e.stderr:
                    logger.error(f"Last error output:\n{e.stderr}")
                failed_chunks.append((idx, chunk_path, str(e)))

        if failed_chunks:
            logger.error(f"\n{len(failed_chunks)} chunks failed:")
            for idx, path, error in failed_chunks:
                logger.error(f"  Batch {idx + 1}: {path} - {error}")
            raise RuntimeError(
                f"{len(failed_chunks)} chunks failed. See log for details and chunk files."
            )

    def _sync_change_type(
            changes: list[ZfsChange] | None, direction: Literal["copy", "delete"],
            state_file: Path | None = None) -> None:
        # Special mode: use pre-generated chunks
        if chunk_dir:
            chunk_pattern = f"st_{cfg.name}_chunk_*.txt"
            chunk_files = sorted(chunk_dir.glob(chunk_pattern))

            if not chunk_files:
                logger.error(f"No chunk files found matching {chunk_pattern} in {chunk_dir}")
                raise typer.Exit(code=1)

            logger.info(f"Found {len(chunk_files)} chunk files for {cfg.name}")

            completed_batches = _load_completed_batches(state_file)

            for idx, chunk_file in enumerate(chunk_files):
                batch_id = f"{chunk_file.name}"
                if batch_id in completed_batches:
                    logger.info(f"Skipping already completed batch: {batch_id}")
                    continue

                logger.info(f"Processing chunk file: {chunk_file}")
                _rclone_batch_sync(chunk_file, direction)

                if state_file:
                    _save_completed_batch(state_file, batch_id)
                    logger.info(f"Saved batch {batch_id} as completed")
        else:
            # Normal mode: generate chunks from ZFS changes
            if changes is None:
                raise ValueError("changes cannot be None when not in chunk_dir mode")
            path_list = _write_files_from_list(cfg, changes, direction)
            _rclone_batch_sync(path_list, direction)

    # Special mode: use pre-generated chunks for testing
    if chunk_dir:
        logger.info(f"Running in chunk directory mode with chunks from {chunk_dir}")
        state_file = STATE_BASE_PATH / f"{cfg.name}-chunk-test.state"

        try:
            # Only run copy direction for chunk testing
            _sync_change_type(None, "copy", state_file)
            logger.success(f"Chunk directory sync completed for {cfg.name}")
        except Exception as e:
            logger.error(f"Chunk directory sync failed: {e}")
            raise
        return  # Exit early - no need to do snapshot verification

    if (previous := _get_verified_snapshot(cfg)) is None:
        mode = FIRST_RUN_MODE[cfg.name]
        logger.info(
            f"No verified snapshot found — using first-run mode: {mode}")
        snap_to_diff = _first_run_behavior(cfg, mode)
        if not snap_to_diff:
            logger.info("First run mode created baseline only — exiting.")
            raise typer.Exit(code=0)
        previous = snap_to_diff

    current = _get_latest_snapshot(cfg)
    if current == previous:
        logger.info("No new snapshot since last verified — nothing to sync.")
        raise typer.Exit(code=0)

    if previous is None:
        # Initial sync - copy everything from the mountpoint
        logger.info(f"Initial sync - copying all data from {cfg.mountpoint}")
        if not dry_run:
            # Use rclone to copy the entire mountpoint (not sync - avoid overhead)
            # For full copy, we need exclude options
            exclude_opts = [
                "--exclude", "**/.gnupg/**",
                "--exclude", "**/.postgres_socket_dir/**",
                "--exclude", "**/.postgres_sock",
                "--exclude", "**/.X11-unix/**",
                "--exclude", "**/*.sock",
                "--exclude", "**/*.socket",
            ]
            cmd = [
                "rclone", "copy",
                f"--config={cfg.remote_rclone_conf_path}",
                *cfg.rclone_opts,
                *exclude_opts,
                str(cfg.mountpoint),
                cfg.remote
            ]
            logger.info(f"Running: {' '.join(cmd)}")
            result = run_ssh(cfg.ssh_host, cmd, capture_output=False)
            logger.success(f"##Verified snapshot##: {current}")
        else:
            logger.info(f"DRY RUN: Would sync entire contents of {cfg.mountpoint} to {cfg.remote}")
        return

    # Normal incremental sync using zfs diff
    changes = _zfs_diff(cfg, previous, current)
    # Filter changes to only those within the mountpoint
    valid_changes = [
        c for c in changes
        if c.relative_to_mountpoint(cfg.mountpoint)
    ]
    # TODO: validate these paths against remote S3 layout to avoid false positives

    _sync_change_type(valid_changes, "copy", None)
    _sync_change_type(valid_changes, "delete", None)

    logger.success(f"##Verified snapshot##: {current}")


def _get_latest_snapshot(cfg: SyncConfig) -> str:
    """Return the name of the most recent snapshot for this dataset."""
    cmd = [
        "zfs", "list", "-t", "snapshot",
        "-o", "name", "-s", "creation", "-H",
        "-d", "1", cfg.dataset
    ]
    result = run_ssh(cfg.ssh_host, cmd, capture_output=True)

    lines = result.stdout.strip().splitlines()
    if not lines:
        raise RuntimeError(f"No snapshots found for dataset: {cfg.dataset}")

    latest = lines[-1]
    logger.info(f"Latest snapshot for {cfg.name} is {latest}")
    return latest


def _load_completed_batches(state_file: Path | None) -> set[str]:
    """Load the set of completed batch IDs from state file."""
    if state_file is None or not state_file.exists():
        return set()

    completed = set()
    with state_file.open() as f:
        for line in f:
            batch_id = line.strip()
            if batch_id:
                completed.add(batch_id)

    return completed


def _save_completed_batch(state_file: Path, batch_id: str) -> None:
    """Append a completed batch ID to the state file."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.open("a") as f:
        f.write(f"{batch_id}\n")


def _get_oldest_snapshot(cfg: SyncConfig) -> str:
    """Return the name of the oldest snapshot for this dataset."""
    cmd = [
        "zfs", "list", "-t", "snapshot",
        "-o", "name", "-s", "creation", "-H",
        "-d", "1", cfg.dataset
    ]
    result = run_ssh(cfg.ssh_host, cmd, capture_output=True)

    lines = result.stdout.strip().splitlines()
    if not lines:
        raise RuntimeError(f"No snapshots found for dataset: {cfg.dataset}")

    oldest = lines[0]  # First line is oldest when sorted by creation
    logger.info(f"Oldest snapshot for {cfg.name} is {oldest}")
    return oldest


app = typer.Typer()

def main(
        path: str = typer.Argument(
            ..., help="Dataset key (e.g. working, backup, legacy)"),
        dry_run: bool = typer.Option(
            False, help="Don't actually sync, just simulate"),
        max_retries: int = typer.Option(
            1, help="Maximum retry attempts per rclone chunk"),
        bootstrap_snapshot: str | None = typer.Option(
            None, help="Snapshot to start from (for backup first-run)"),
        chunk_dir: Path | None = typer.Option(
            None, help="Directory containing pre-generated chunk files for testing")):
    """Sync ZFS snapshot diffs to encrypted S3 via rclone."""
    # Setup logging for this specific path
    log_path = LOG_BASE_PATH / f"{path}.log"
    LOG_BASE_PATH.mkdir(parents=True, exist_ok=True)
    STATE_BASE_PATH.mkdir(parents=True, exist_ok=True)
    setup_logging(log_file=str(log_path))

    lockfile = acquire_lock(f"/var/lock/nas-to-st-{path}.lock")

    # Validate bootstrap snapshot is only used for backup dataset
    if bootstrap_snapshot and path != "backup":
        logger.error("bootstrap_snapshot can only be used with 'backup' dataset")
        raise typer.Exit(code=1)

    if path == "backup" and not bootstrap_snapshot:
        # Check if this is a first run that would need a bootstrap snapshot
        # For backup dataset, always use stbackup:hrdag
        verified = _get_verified_snapshot(SyncConfig(
            name=path,
            dataset=f"hrdag-nas/nas/{path}",
            mountpoint=Path(f"/mnt/hrdag-nas/nas/{path}"),
            remote="stbackup:hrdag",
            control_rclone_conf_path=Path("/mnt/credsDrive/rclone.conf"),
            ssh_host="nas"
        ))
        if not verified:
            logger.error("First run of 'backup' dataset requires --bootstrap-snapshot")
            logger.info("Example: --bootstrap-snapshot 'hrdag-nas/nas/backup@2025-05-20T00:00:00-07:00'")
            raise typer.Exit(code=1)

    # TODO: remote = f"st{path}:"  # <-- is enough
    remote_mapping = MappingProxyType({
        "test": "sttest:",
        "working": "stworking:",
        "legacy": "stlegacy:",
        "backup": "stbackup:"
    })
    remote = remote_mapping.get(path, f"st{path}:")  # Default pattern if not in mapping

    cfg = SyncConfig(
        name=path,
        dataset=f"hrdag-nas/nas/{path}",
        mountpoint=Path(f"/mnt/hrdag-nas/nas/{path}"),
        remote=remote,
        control_rclone_conf_path=Path("/mnt/credsDrive/rclone.conf"),
        # rclone_opts is now set via default in SyncConfig
        ssh_host="nas",
        bootstrap_snapshot=bootstrap_snapshot
    )
    sync_path(cfg, dry_run=dry_run, max_retries=max_retries, chunk_dir=chunk_dir)

    release_lock(lockfile)


if __name__ == "__main__":
    handle_exit()
    typer.run(main)
