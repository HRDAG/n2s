#!/bin/bash
# Script to set up git hooks for n2s development
# Run this after cloning the repository to install development hooks

echo "Setting up n2s development git hooks..."

# Create pre-commit hook to update README CLI help
cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
# Pre-commit hook to update README with latest CLI help output

echo "Updating README with latest CLI help output..."
python3 scripts/update-readme-cli-help.py

# Check if README was modified and if so, add it to the commit
if git diff --cached --name-only | grep -q "README.md"; then
    echo "README.md CLI help section updated"
fi
EOF

# Make the hook executable
chmod +x .git/hooks/pre-commit

echo "✅ Pre-commit hook installed successfully!"
echo "   - README.md will be automatically updated with CLI help output on each commit"
echo "   - The hook uses scripts/update-readme-cli-help.py to sync CLI help"