#!/bin/bash

# 1. Create top-level directories
mkdir src tests config data notebooks dist

# 2. Create the core source code subdirectories (-p creates parents if they don't exist)
mkdir -p src/agent src/data_quality src/retrieval src/api

# 3. Create essential placeholder files (e.g., __init__.py for modules)
touch README.md
touch pyproject.toml
touch src/agent/__init__.py
touch src/data_quality/__init__.py
touch src/retrieval/__init__.py
touch src/api/main.py
touch config/settings.yaml
touch tests/test_placeholder.py

# 4. Create .gitignore (CRITICAL for Poetry/venv)
cat <<EOF > .gitignore
# Dependency Management
*.pyc
*.o
*.so
*~

# Python Virtual Environment
.venv/
__pycache__/

# Distribution
dist/

# Logs and Data
*.log
/data/*
EOF

echo "Project structure and .gitignore created successfully."
