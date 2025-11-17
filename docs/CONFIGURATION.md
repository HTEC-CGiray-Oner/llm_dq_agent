# Configuration Guide

## Overview

The LLM Data Quality Agent uses a two-layer configuration system:
1. **settings.yaml** - Application settings, connector defaults, schema discovery config
2. **.env** - Sensitive credentials (API keys, passwords)

---

## Configuration Files

### 1. config/settings.yaml

Main configuration file for application settings.

```yaml
# Default connector to use (snowflake, postgres, csv)
default_connector: csv

# Connector configurations
connectors:
  snowflake:
    # Schema discovery settings (for automatic table indexing)
    discovery:
      database: null  # null = use current database from connector config
      schema: null    # null = use current schema from connector config
      # Or specify explicitly:
      # database: AGENT_LLM_READ
      # schema: PUBLIC

  postgres:
    discovery:
      database: null
      schema: public

  csv:
    base_path: ./data

# LLM settings
llm:
  model: l2-gpt-4o
  temperature: 0

# RAG settings
rag:
  embedding_model: all-MiniLM-L6-v2
  top_k: 3
  vector_db_path: ./chroma_db
```

### 2. .env

Sensitive credentials that should NOT be committed to version control.

```bash
# OpenAI-compatible API
OPENAI_API_KEY=your_api_key_here
OPENAI_BASE_URL=https://your-llm-service.com

# Snowflake credentials
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role  # optional

# PostgreSQL credentials
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

---

## Schema Discovery Configuration

### Option 1: Use Config File (Recommended)

**config/settings.yaml:**
```yaml
connectors:
  snowflake:
    discovery:
      database: AGENT_LLM_READ
      schema: PUBLIC
```

**Python code:**
```python
from src.retrieval.schema_indexer import build_schema_index_for_snowflake

# Uses database and schema from settings.yaml
build_schema_index_for_snowflake()
```

### Option 2: Use Current Connection Defaults

**config/settings.yaml:**
```yaml
connectors:
  snowflake:
    discovery:
      database: null  # Uses current database from .env
      schema: null    # Uses current schema from .env
```

**.env:**
```bash
SNOWFLAKE_DATABASE=AGENT_LLM_READ
SNOWFLAKE_SCHEMA=PUBLIC
```

**Python code:**
```python
# Uses current connection database/schema
build_schema_index_for_snowflake()
```

### Option 3: Override in Code

```python
# Explicit override - ignores config
build_schema_index_for_snowflake(
    database='MY_DATABASE',
    schema='MY_SCHEMA'
)
```

### Option 4: Index Multiple Schemas

```python
from src.retrieval.schema_indexer import SchemaIndexer

indexer = SchemaIndexer('snowflake')

# Index first schema
indexer.build_schema_index(
    database='AGENT_LLM_READ',
    schema='PUBLIC',
    recreate=True  # Create new index
)

# Add more schemas to same index
indexer.build_schema_index(
    database='AGENT_LLM_READ',
    schema='ANALYTICS',
    recreate=False  # Append to existing index
)

indexer.build_schema_index(
    database='AGENT_LLM_READ',
    schema='STAGING',
    recreate=False  # Append
)
```

---

## Configuration Priority

Settings are loaded in this order (later overrides earlier):

1. **Default values** in code
2. **settings.yaml** configuration
3. **.env** environment variables
4. **Explicit parameters** in function calls

### Example:

```yaml
# settings.yaml
connectors:
  snowflake:
    discovery:
      schema: PUBLIC
```

```python
# Uses PUBLIC from settings.yaml
build_schema_index_for_snowflake()

# Overrides to use ANALYTICS
build_schema_index_for_snowflake(schema='ANALYTICS')
```

---

## Common Configurations

### Development Setup

**settings.yaml:**
```yaml
default_connector: csv  # Use CSV files during development

connectors:
  snowflake:
    discovery:
      schema: DEV_SCHEMA  # Development schema
```

### Production Setup

**settings.yaml:**
```yaml
default_connector: snowflake  # Use Snowflake in production

connectors:
  snowflake:
    discovery:
      database: PROD_DB
      schema: PUBLIC
```

### Testing Setup

**settings.yaml:**
```yaml
connectors:
  snowflake:
    discovery:
      schema: TEST_SCHEMA
```

**Python:**
```python
# Limit tables for fast testing
build_schema_index_for_snowflake(max_tables=5)
```

---

## Environment-Specific Configuration

### Using Multiple Environments

Create different config files:

```
config/
  settings.yaml          # Default
  settings.dev.yaml      # Development
  settings.prod.yaml     # Production
```

Load the appropriate config:

```python
import yaml
import os

env = os.getenv('APP_ENV', 'dev')
config_file = f'config/settings.{env}.yaml'

with open(config_file) as f:
    settings = yaml.safe_load(f)
```

---

## Updating Configuration

### Change Schema to Index

1. Edit **config/settings.yaml**:
   ```yaml
   connectors:
     snowflake:
       discovery:
         schema: NEW_SCHEMA
   ```

2. Rebuild index:
   ```bash
   poetry run python src/retrieval/schema_indexer.py
   ```

### Add New Database

1. Add to **.env**:
   ```bash
   SNOWFLAKE_DATABASE=NEW_DATABASE
   ```

2. Update **settings.yaml**:
   ```yaml
   connectors:
     snowflake:
       discovery:
         database: NEW_DATABASE
   ```

3. Rebuild index

---

## Troubleshooting

### "Using wrong schema"

Check config priority:
1. Function parameters (highest priority)
2. settings.yaml discovery config
3. .env connection defaults (lowest priority)

### "Tables not found"

Ensure discovery config matches your connection:

```yaml
# If .env has:
SNOWFLAKE_SCHEMA=PUBLIC

# Then settings.yaml should have:
discovery:
  schema: PUBLIC  # Match your connection
```

### "Config not loading"

Check file paths:
```python
# settings.yaml should be at:
# project_root/config/settings.yaml

# .env should be at:
# project_root/.env
```

---

## Best Practices

1. **Never commit .env** - Add to .gitignore
2. **Use settings.yaml for non-sensitive config** - Safe to commit
3. **Document your schema** - Add comments to Snowflake tables
4. **Use null for flexibility** - Let connection defaults work
5. **Explicit is better for multi-schema** - Specify database/schema when indexing multiple schemas

---

## Examples

### Single Schema (Simple)

**settings.yaml:**
```yaml
connectors:
  snowflake:
    discovery:
      schema: PUBLIC
```

**Usage:**
```python
build_schema_index_for_snowflake()  # Just works!
```

### Multiple Schemas (Advanced)

**Python:**
```python
indexer = SchemaIndexer('snowflake')

for schema in ['PUBLIC', 'ANALYTICS', 'REPORTING']:
    indexer.build_schema_index(
        schema=schema,
        recreate=(schema == 'PUBLIC')  # First one creates, rest append
    )
```

### Dynamic Configuration

**Python:**
```python
import os

# Get from environment
schema = os.getenv('TARGET_SCHEMA', 'PUBLIC')

build_schema_index_for_snowflake(schema=schema)
```
