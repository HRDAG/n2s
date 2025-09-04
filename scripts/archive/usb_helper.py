#!/usr/bin/env python3
"""
Helper script to access USB volumes when FDA doesn't work over SSH.
Runs commands locally via osascript to bypass SSH FDA limitations.
"""
import subprocess
import sys
import json

def run_local_command(cmd):
    """Run a command locally via osascript."""
    apple_script = f'do shell script "{cmd}"'
    result = subprocess.run(
        ["osascript", "-e", apple_script],
        capture_output=True,
        text=True
    )
    return result.stdout, result.stderr, result.returncode

def list_files(path, limit=None):
    """List files in a USB volume path."""
    cmd = f"ls -la '{path}'"
    if limit:
        cmd += f" | head -{limit}"
    
    stdout, stderr, code = run_local_command(cmd)
    if code == 0:
        return stdout.strip().split('\n') if stdout else []
    else:
        return []

def check_file_exists(filepath):
    """Check if a file exists on USB."""
    cmd = f"test -f '{filepath}' && echo 'exists' || echo 'not found'"
    stdout, stderr, code = run_local_command(cmd)
    return 'exists' in stdout

def read_file(filepath, lines=None):
    """Read a file from USB."""
    if lines:
        cmd = f"head -{lines} '{filepath}'"
    else:
        cmd = f"cat '{filepath}'"
    
    stdout, stderr, code = run_local_command(cmd)
    return stdout if code == 0 else None

if __name__ == "__main__":
    # Test the helper
    print("Testing USB access via osascript...")
    files = list_files("/Volumes/backup", limit=5)
    if files:
        print(f"Found {len(files)} items in /Volumes/backup")
        for f in files[:5]:
            print(f"  {f}")
    else:
        print("Could not access /Volumes/backup or it's empty")