# this is a giant mess. it needs to be completely revisited, part by part.
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterator, Literal
from zoneinfo import ZoneInfo

from loguru import logger
import subprocess
import typer

# --- Simulated .types ---
from pydantic import BaseModel
from enum import Enum
import tempfile
import uuid
import shutil
class SyncConfig(BaseModel):
    name: str
    dataset: str
    mountpoint: Path
    remote: str
    control_rclone_conf_path: Path
    rclone_opts: list[str] = [
        "--bwlimit", "50M",
        "--transfers", "64",
        "--checkers", "128",
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
        "--exclude", "'**/.gnupg/**'",
        "--exclude", "'**/.postgres_socket_dir/**'",
        "--exclude", "'**/.postgres_sock'",
        "--exclude", "'**/.X11-unix'",
        "--progress",
    ]
    ssh_host: str
    remote_rclone_conf_path: Path = Path()

    def model_post_init(self, __context):
        # TODO: validate that control_rclone_conf_path exists before attempting to copy
        tmpname = f"/tmp/rclone_{uuid.uuid4().hex}.conf"
        self.remote_rclone_conf_path = Path(tmpname)
        try:
            shutil.copy(self.control_rclone_conf_path, self.remote_rclone_conf_path)
            subprocess.run([
                "scp", str(self.control_rclone_conf_path),
                f"{self.ssh_host}:{self.remote_rclone_conf_path}"
            ], check=True)
        except Exception as e:
            raise RuntimeError(f"Failed to copy rclone config to NAS: {e}")







class ZfsAction(str, Enum):
    MODIFIED = "M"
    ADDED = "+"
    REMOVED = "-"
    OTHER = "?"


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


# --- Simulated .zfs ---
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


# --- Simulated .files ---
def _write_files_from_list(cfg: SyncConfig, changes: list[ZfsChange], direction: Literal["copy", "delete"]) -> Path:
    target = Path(f"/tmp/{cfg.name}_{direction}_list.txt")
    if direction == "copy":
        filtered = [c.relative_to_mountpoint(cfg.mountpoint) for c in changes if c.action in {ZfsAction.MODIFIED, ZfsAction.ADDED}]
    else:
        filtered = [c.relative_to_mountpoint(cfg.mountpoint) for c in changes if c.action == ZfsAction.REMOVED]
    filtered = [p for p in filtered if p is not None]
    with target.open("w") as f:
        f.writelines(f"{p}\n" for p in filtered)
    return target


from types import MappingProxyType

# --- Simulated .logfile ---
FIRST_RUN_MODE = MappingProxyType({
    "backup": "diff_from_explicit",
    "working": "copy_all",
    "legacy": "baseline_only"
})


def _get_verified_snapshot(cfg: SyncConfig) -> str | None:
    log_path = Path(f"/var/nas-to-st/{cfg.name}.log")
    if not log_path.exists():
        return None
    with log_path.open() as f:
        for line in reversed(f.readlines()):
            if "##Verified snapshot##:" in line:
                return line.split("##Verified snapshot##:", 1)[1].strip()
    return None


# TODO: implement real snapshot name resolution based on current time or history
# TODO: log 'see you tomorrow' on initial baseline

def _first_run_behavior(cfg: SyncConfig, mode: str) -> str | None:
    if mode == "copy_all":
        logger.info("No previous snapshot — copying everything")
        return "baseline"
    elif mode == "baseline_only":
        logger.success("Baseline recorded, no sync needed")
        return None
    elif mode == "diff_from_explicit":
        logger.info("Using configured bootstrap snapshot")
        return "bootstrap"
    raise ValueError(f"Unknown first run mode: {mode}")


# --- Simulated .remote ---
def run_ssh(host: str, cmd: list[str], capture_output: bool = False):
    full_cmd = ["ssh", host] + cmd
    if capture_output:
        return subprocess.run(full_cmd, text=True, capture_output=True, check=True)
    else:
        return subprocess.run(full_cmd, check=True)


# --- hrdaglib integration ---
from hrdaglib import setup_logging, handle_exit, acquire_lock, release_lock


# --- sync_path logic ---
def sync_path(
    cfg: SyncConfig,
    dry_run: bool = False,
    max_retries: int = 1
) -> None:
    def _rclone_batch_sync(files_from: Path, direction: Literal["copy", "delete"]) -> None:
        def _yield_files_from_chunks(source: Path, chunk_size: int = 10_000) -> Iterator[Path]:
            def _write_chunk(idx: int, chunk: list[str]) -> Path:
                path = Path(f"/tmp/rclone_{direction}_{cfg.name}_{idx:03d}.txt")
                with path.open("w") as f:
                    f.writelines(f"{line}
" for line in chunk)
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
            str(cfg.mountpoint) if direction == "copy" else cfg.remote,
            cfg.remote if direction == "copy" else "--config",
            str(cfg.remote_rclone_conf_path),
            "--no-traverse",
        ]
        if direction == "delete":
            base_cmd.insert(4, "--rmdirs")
        if dry_run:
            base_cmd.append("--dry-run")
        base_cmd += cfg.rclone_opts

        for idx, chunk_path in enumerate(_yield_files_from_chunks(files_from)):
            cmd = base_cmd + ["--files-from", str(chunk_path)]
            for attempt in range(1, max_retries + 2):
                logger.info(f"Running rclone {direction} batch {idx + 1}, attempt {attempt}")
                result = subprocess.run(cmd)
                if result.returncode == 0:
                    logger.success(f"{direction.title()} batch {idx + 1} complete")
                    chunk_path.unlink(missing_ok=True)
                    logger.debug(f"Deleted temporary chunk file: {chunk_path}")
                    break
                elif attempt <= max_retries:
                    logger.warning(f"Batch {idx + 1} failed (rc={result.returncode}), retrying...")
                else:
                    logger.error(f"Batch {idx + 1} failed after {max_retries + 1} attempts.")
                    logger.error(f"Failed chunk file saved at: {chunk_path}")
                    raise RuntimeError(f"Failed batch {idx + 1}, see: {chunk_path}")

    def _sync_change_type(changes: list[ZfsChange], direction: Literal["copy", "delete"]) -> None:
        path_list = _write_files_from_list(cfg, changes, direction)
        _rclone_batch_sync(path_list, direction)

    if (previous := _get_verified_snapshot(cfg)) is None:
        mode = FIRST_RUN_MODE[cfg.name]
        logger.info(f"No verified snapshot found — using first-run mode: {mode}")
        snap_to_diff = _first_run_behavior(cfg, mode)
        if not snap_to_diff:
            logger.info("First run mode created baseline only — exiting.")
            raise typer.Exit(code=0)
        previous = snap_to_diff

    current = _get_latest_snapshot(cfg)
    if current == previous:
        logger.info("No new snapshot since last verified — nothing to sync.")
        raise typer.Exit(code=0)

    changes = _zfs_diff(cfg, previous, current)
    rel_changes = [
        # TODO: validate these paths against remote S3 layout to avoid false positives
        rel for c in changes
        if (rel := c.relative_to_mountpoint(cfg.mountpoint))
    ]

    _sync_change_type(rel_changes, "copy")
    _sync_change_type(rel_changes, "delete")

    logger.success(f"##Verified snapshot##: {current}")


# --- Existing code ---
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


# --- Typer CLI ---
app = typer.Typer()

# TODO: remove invalid decorator stacking, use callback or command separately depending on CLI design
@app.callback()
# TODO: move logging output to per-path files (e.g., /var/nas-to-st/{path}.log)
def setup():
    setup_logging()
    handle_exit()

@app.command()
def main(
    path: str = typer.Argument(..., help="Dataset key (e.g. working, backup, legacy)"),
    dry_run: bool = typer.Option(False, help="Don't actually sync, just simulate"),
    max_retries: int = typer.Option(1, help="Maximum retry attempts per rclone chunk")
):
    """Sync ZFS snapshot diffs to encrypted S3 via rclone."""
    lockfile = acquire_lock(f"/tmp/nas-to-st-{path}.lock")

    cfg = SyncConfig(
        name=path,
        dataset=f"hrdag-nas/nas/{path}",
        mountpoint=Path(f"/mnt/hrdag-nas/nas/{path}"),
        remote="stbackup:hrdag",
        control_rclone_conf_path=Path("/etc/rclone/rclone.conf"),
        # rclone_opts is now set via default in SyncConfig
        ssh_host="nas"  # Replace with your actual hostname
    )
    sync_path(cfg, dry_run=dry_run, max_retries=max_retries)


    release_lock(lockfile)

if __name__ == "__main__":
    app()
