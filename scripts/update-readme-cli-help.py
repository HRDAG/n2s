#!/usr/bin/env python3
"""Update CLI help output in README.md"""

import subprocess
import re
import sys

# Get the CLI help output
result = subprocess.run(['uv', 'run', 'n2s', '--help'], 
                      capture_output=True, text=True)
cli_help = result.stdout

# Read the current README
with open('README.md', 'r') as f:
    readme_content = f.read()

# Find the section to replace - note the exact formatting with spaces
pattern = r'(<!--- CLI help output start --->\n```\n)(.*?)(```\n<!--- CLI help output end --->)'

# Format CLI help with proper indentation
formatted_help = cli_help + '\n'

# Replace the section
replacement = r'\1' + formatted_help + r'\3'
new_content = re.sub(pattern, replacement, readme_content, flags=re.DOTALL)

# Write back to README only if changed
if new_content != readme_content:
    with open('README.md', 'w') as f:
        f.write(new_content)
    print("README.md updated with latest CLI help")
    subprocess.run(['git', 'add', 'README.md'])
else:
    print("README.md is up to date")