# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# CLAUDE.md

# Development Guidelines for n2s Project

## 1. Code Changes
- NO changes to main code without explicit approval
- Test files can be modified freely
- Always propose changes before implementing
- Exploratory commands (ls, find, grep, cat, etc.) can be run freely without approval
- Stop and ask before making any code changes
- Comments explain WHY, not WHAT
- Keep functions focused and single-purpose
- Always define constants in ALL_CAPS and use appropriate frozenset, frozen=True, typing.Final, MappingProxyType, typing.Literal, typing.ReadOnly or similar features to prevent changes
- Assume python >=3.13 respecting this version's type annotation and other features
- Don't propose a commit until all pytest passes cleanly
- Follow existing code style and patterns
- Seek minimal code refactors unless discussed with me first

## 2. Development Flow
- Take one step at a time
- Write/update tests first to define behavior
- Avoid mocking when possible, use real function calls and fixtures
- Get tests passing before moving to next step
- Each step should be small and verifiable
- Refactor only after tests pass
- Consider edge cases before marking a task complete

## 3. Communication
- Keep communication direct and professional without unnecessary politeness
- Ask clarifying questions when specifications are unclear
- We operate as peers with mutual respect
- Keep proposals and their rationale together
- Be explicit about assumptions
- Flag potential backwards compatibility issues
- Sign commit messages "By PB & Claude"
- Remove any "Generated" lines and change to "Co-authored-by: Claude and PB"

## 4. File Headers
Every file should begin with:
```
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# relative/path/to/file  <-- relative to git root
```

## 5. Analysis
- Use Claude Sonnet 4 as default model (upgrade to Opus 4 only if needed)
- Analyze implications before implementing changes
- Consider performance, error handling, and security implications
- If unsure, gather more information or ask

## 6. Format
When showing failures and fixes, use this structure:
```
Failures:
1. [test name] - [error]
   FIX: [proposed solution]
```

## 7. Testing Strategy
- Focus on unit tests, integration tests where appropriate
- Minimal end-to-end testing
- Use real function calls and fixtures over mocks when possible
- Test file naming: `test_<module>.py`
- Test class naming: `TestClassName`
- Test method naming: `test_method_description`

## 8. Code Style & Formatting
- Line length: 79 characters
- Use black with `--line-length 79`
- Import ordering (3 groups with blank lines between):
  1. Python builtins
  2. External dependencies (what uv manages)
  3. Local/personal libraries not in PyPI
- Google-style docstrings
- Type hints for all functions and methods

## 9. Error Handling & Logging
- Use loguru for logging
- Use Typer for CLI exits and error messages
- Structured logging with appropriate levels
- Clear, actionable error messages

## 10. Git Workflow
- Check for pre-commit hooks (may update README with CLI)
- Always run pre-commit hooks before proposing commits
- Descriptive commit messages explaining WHY, not just what

## 11. Database (PostgreSQL)
- TBD - await further specifications
- Consider using migrations
- Connection handling patterns to be determined

## Commands to run before proposing commits:
```bash
pytest
mypy .
ruff check .
black --check . --line-length 79
# Check for pre-commit hooks:
pre-commit run --all-files  # if .pre-commit-config.yaml exists
```

## Project Dependencies
- pydantic>=2.11.5 - Data validation
- pyrage>=1.2.5 - Encryption
- typer>=0.16.0 - CLI framework
- loguru - Logging (to be added)
- pytest>=8.3.5 - Testing
- black>=25.1.0 - Code formatting
- mypy>=1.15.0 - Type checking
- ruff>=0.11.11 - Linting