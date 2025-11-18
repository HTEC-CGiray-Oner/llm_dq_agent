# Migration Guide: planner.py → smart_planner.py

## Overview

The project has migrated from `planner.py` (basic agent with manual table specification) to `smart_planner.py` (enhanced agent with automatic table discovery and multi-connector support).

## What Changed?

### Old Approach (`planner.py`) - REMOVED ❌
- **Manual table specification**: User had to know exact table names
- **RAG for tool documentation**: Used vector DB to retrieve descriptions of DQ functions
- **Single-connector focus**: Didn't enforce connector type parameter
- **Limited discovery**: Agent didn't know what tables exist

### New Approach (`smart_planner.py`) - ACTIVE ✅
- **Automatic table discovery**: Semantic search finds tables based on natural language
- **Multi-connector aware**: Distinguishes between Snowflake, PostgreSQL, etc.
- **Schema metadata**: Indexes table schemas (columns, types, row counts) as documents
- **Smart matching**: "snowflake customers" vs "postgres customers"

## Migration Steps

### 1. Update Imports

**Before:**
```python
from src.agent.planner import run_dq_check
```

**After:**
```python
from src.agent.smart_planner import run_smart_dq_check
```

### 2. Update Function Calls

**Before:**
```python
# Required exact table name
result = run_dq_check("Check for duplicates in Snowflake table AGENT_LLM_READ.PUBLIC.CUSTOMERS")
```

**After:**
```python
# Natural language with automatic discovery
result = run_smart_dq_check("Check for duplicates in Snowflake customers table")
```

### 3. Build Schema Index (One-Time Setup)

The new system requires indexing your table schemas first:

```python
from src.retrieval.schema_indexer import SchemaIndexer

# Index Snowflake tables
indexer = SchemaIndexer(connector_type='snowflake')
indexer.build_schema_index(recreate=True)

# Index PostgreSQL tables (append)
indexer = SchemaIndexer(connector_type='postgres')
indexer.build_schema_index(recreate=False)
```

Or use the interactive demo:
```bash
poetry run python examples/schema_discovery_demo.py
```

## Key Benefits

### 1. No More Exact Table Names Required

**Before:**
```python
# Had to know: AGENT_LLM_READ.PUBLIC.CUSTOMERS
run_dq_check("Check AGENT_LLM_READ.PUBLIC.CUSTOMERS for duplicates")
```

**After:**
```python
# Just describe what you want
run_smart_dq_check("Check customer data for duplicates")
```

### 2. Multi-Connector Support

**Before:**
```python
# Connector type was optional, relied on default
run_dq_check("Check customers table")
```

**After:**
```python
# Explicitly distinguish between connectors
run_smart_dq_check("Check snowflake customers table")
run_smart_dq_check("Check postgres customers table")
```

### 3. Automatic Table Discovery

**Before:**
```python
# You had to know what tables exist
run_dq_check("Check CUSTOMERS table")
```

**After:**
```python
# Agent discovers and ranks relevant tables
run_smart_dq_check("Find customer contact information and check for duplicates")
# → Automatically finds CUSTOMERS table based on column names and descriptions
```

## Configuration Requirements

### Prerequisites
1. Install connectors: `poetry install -E snowflake -E postgres`
2. Configure credentials in `.env` and `config/settings.yaml`
3. Build schema index (one-time): Run `schema_discovery_demo.py` → Option 1

### Schema Indexing
The new system indexes table metadata into ChromaDB:
- **Location**: `./chroma_db`
- **Collection**: `database_schemas`
- **Embeddings**: HuggingFace all-MiniLM-L6-v2 (384-dim)

To rebuild index:
```python
indexer = SchemaIndexer(connector_type='snowflake')
indexer.build_schema_index(recreate=True)  # Recreate from scratch
```

To add more tables:
```python
indexer = SchemaIndexer(connector_type='postgres')
indexer.build_schema_index(recreate=False)  # Append mode
```

## Backward Compatibility

⚠️ **Breaking Change**: `planner.py` and `index_builder.py` have been **removed**.

If you have custom code using these modules, you must migrate to `smart_planner.py`.

## Troubleshooting

### Error: "No relevant tables found"
**Solution**: Build the schema index first
```bash
poetry run python examples/schema_discovery_demo.py
# Choose Option 1 to build index
```

### Error: "Database 'POSTGRES' does not exist"
**Problem**: Agent using wrong connector for PostgreSQL table

**Solution**: Already fixed! The agent now correctly passes `connector_type` parameter based on discovered table metadata.

### Tables Not Appearing in Search
**Solution**: Rebuild index with `recreate=True`
```python
indexer = SchemaIndexer(connector_type='your_connector')
indexer.build_schema_index(recreate=True)
```

## Files Updated

### Code Files
- ✅ `examples/connector_usage.py` → Now uses `smart_planner`
- ✅ `notebooks/tryouts.ipynb` → Now uses `smart_planner`
- ✅ `examples/schema_discovery_demo.py` → Already using `smart_planner`

### Documentation
- ✅ `docs/CONNECTORS.md` → Updated examples
- ✅ `docs/FEATURE_SUMMARY.md` → Updated examples

### Removed Files
- ❌ `src/agent/planner.py` → Replaced by `smart_planner.py`
- ❌ `src/retrieval/index_builder.py` → Replaced by `schema_indexer.py`

## Next Steps

1. **Run schema discovery demo**: `poetry run python examples/schema_discovery_demo.py`
2. **Index your tables**: Choose Option 1 in the demo
3. **Test queries**: Try natural language queries in Option 3
4. **Update your code**: Replace `run_dq_check` with `run_smart_dq_check`

## Questions?

- Review `examples/schema_discovery_demo.py` for complete workflow
- Check `docs/MULTI_CONNECTOR_INDEXING.md` for indexing details
- See `src/agent/smart_planner.py` for implementation details
