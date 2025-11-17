# Automatic Schema Discovery

The system now **automatically discovers all schemas** from Snowflake's INFORMATION_SCHEMA and indexes all tables.

## Quick Start

### 1. Enable Auto-Discovery (config/settings.yaml)

```yaml
connectors:
  snowflake:
    discovery:
      auto_discover_schemas: true  # ✓ Enabled by default
      exclude_schemas: []          # Optional: skip specific schemas
```

### 2. Run Indexing

```bash
poetry run python src/retrieval/schema_indexer.py
```

**What happens:**
1. Queries `INFORMATION_SCHEMA.SCHEMATA` to find all schemas
2. For each schema, queries `INFORMATION_SCHEMA.TABLES` to find all tables
3. Extracts metadata (columns, types, row counts, descriptions)
4. Embeds and indexes all table metadata
5. Creates single searchable vector database

---

## Configuration Options

### Option 1: Auto-Discover All Schemas (Default)

**settings.yaml:**
```yaml
discovery:
  auto_discover_schemas: true
  exclude_schemas: []  # Optional
```

**Result:** Indexes ALL schemas in database

### Option 2: Exclude Specific Schemas

**settings.yaml:**
```yaml
discovery:
  auto_discover_schemas: true
  exclude_schemas: [TEMP, STAGING_OLD, ARCHIVE]
```

**Result:** Indexes all except excluded schemas

### Option 3: Specific Schemas Only

**settings.yaml:**
```yaml
discovery:
  auto_discover_schemas: false
  schemas: [PUBLIC, ANALYTICS, REPORTING]
```

**Result:** Only indexes specified schemas

### Option 4: Single Schema

**settings.yaml:**
```yaml
discovery:
  auto_discover_schemas: false
  schema: PUBLIC
```

**Result:** Only indexes PUBLIC schema

---

## Python API

### Auto-Discover All Schemas

```python
from src.retrieval.schema_indexer import SchemaIndexer

indexer = SchemaIndexer('snowflake')

# Uses config setting (auto_discover_schemas: true)
indexer.build_schema_index()
```

### Override in Code

```python
# Explicit auto-discovery
indexer.build_schema_index(auto_discover_schemas=True)

# Specific schemas
indexer.build_schema_index(schemas=['PUBLIC', 'ANALYTICS'])

# Single schema
indexer.build_schema_index(schema='PUBLIC')
```

---

## How It Works

### Schema Discovery Query

```sql
SELECT SCHEMA_NAME
FROM {database}.INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
ORDER BY SCHEMA_NAME
```

### Table Discovery Query (per schema)

```sql
SELECT
    TABLE_CATALOG as database_name,
    TABLE_SCHEMA as schema_name,
    TABLE_NAME as table_name,
    TABLE_TYPE as table_type,
    ROW_COUNT,
    COMMENT as table_comment
FROM {database}.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '{schema}'
ORDER BY TABLE_NAME
```

### Column Discovery Query (per table)

```sql
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COMMENT as column_comment
FROM {database}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{schema}'
  AND TABLE_NAME = '{table}'
ORDER BY ORDINAL_POSITION
```

---

## Example Output

```
======================================================================
BUILDING SCHEMA INDEX
======================================================================
Auto-discovered 3 schemas: PUBLIC, ANALYTICS, REPORTING

======================================================================
[1/3] INDEXING SCHEMA: PUBLIC
======================================================================
✓ Connected to Snowflake: AGENT_LLM_READ.PUBLIC
Discovering tables in AGENT_LLM_READ.PUBLIC...
✓ Found 5 tables/views
✓ Disconnected from Snowflake

Generating metadata documents for 5 tables...
  [1/5] Processing CUSTOMERS...
  [2/5] Processing ORDERS...
  [3/5] Processing PRODUCTS...
  [4/5] Processing INVOICES...
  [5/5] Processing PAYMENTS...
✓ Generated 5 metadata documents

Indexing 5 table metadata documents...
✓ Indexed 5 tables from schema PUBLIC

======================================================================
[2/3] INDEXING SCHEMA: ANALYTICS
======================================================================
✓ Connected to Snowflake: AGENT_LLM_READ.ANALYTICS
Discovering tables in AGENT_LLM_READ.ANALYTICS...
✓ Found 3 tables/views
✓ Disconnected from Snowflake

Generating metadata documents for 3 tables...
  [1/3] Processing SALES_SUMMARY...
  [2/3] Processing REVENUE_METRICS...
  [3/3] Processing CUSTOMER_SEGMENTS...
✓ Generated 3 metadata documents

Indexing 3 table metadata documents...
✓ Indexed 3 tables from schema ANALYTICS

======================================================================
[3/3] INDEXING SCHEMA: REPORTING
======================================================================
✓ Connected to Snowflake: AGENT_LLM_READ.REPORTING
Discovering tables in AGENT_LLM_READ.REPORTING...
✓ Found 2 tables/views
✓ Disconnected from Snowflake

Generating metadata documents for 2 tables...
  [1/2] Processing DAILY_REPORTS...
  [2/2] Processing MONTHLY_KPI...
✓ Generated 2 metadata documents

Indexing 2 table metadata documents...
✓ Indexed 2 tables from schema REPORTING

======================================================================
✓ SCHEMA INDEX BUILT SUCCESSFULLY
  - Collection: database_schemas
  - Schemas indexed: 3
  - Total tables indexed: 10
  - Path: ./chroma_db
======================================================================
```

---

## Querying Across Schemas

Once indexed, the agent can find tables across **all schemas**:

```python
from src.agent.smart_planner import run_smart_dq_check

# Agent searches across all indexed schemas
result = run_smart_dq_check("Check for duplicates in customer data")

# Might find:
# - PUBLIC.CUSTOMERS
# - ANALYTICS.CUSTOMER_SEGMENTS
# - REPORTING.CUSTOMER_SUMMARY
```

---

## Benefits

✅ **Zero manual configuration** - Just enable and run
✅ **Always up-to-date** - Discovers new schemas automatically
✅ **Cross-schema search** - Agent finds tables anywhere
✅ **Flexible filtering** - Exclude unwanted schemas
✅ **Single vector database** - All tables searchable together

---

## Troubleshooting

### "No schemas found"

Check Snowflake permissions:
```sql
-- Must have access to INFORMATION_SCHEMA
SHOW GRANTS TO USER your_username;
```

### "Some schemas missing"

Check `exclude_schemas` in settings.yaml:
```yaml
exclude_schemas: []  # Make sure not excluding wanted schemas
```

### "Too many tables"

Limit per schema:
```python
indexer.build_schema_index(
    auto_discover_schemas=True,
    max_tables=50  # Per schema
)
```

---

## Advanced Usage

### Incremental Updates

```python
# Add new schema without recreating
indexer.build_schema_index(
    schema='NEW_SCHEMA',
    recreate=False  # Append to existing index
)
```

### Custom Schema List

```python
# Get all schemas first
schemas = indexer._get_all_schemas()
print(f"Available: {schemas}")

# Filter and index
filtered = [s for s in schemas if 'PROD' in s]
indexer.build_schema_index(schemas=filtered)
```

### Database Override

```python
# Index from different database
indexer.build_schema_index(
    database='OTHER_DATABASE',
    auto_discover_schemas=True
)
```

---

## Performance

**Typical indexing times:**
- 1 schema with 10 tables: ~30 seconds
- 3 schemas with 30 tables: ~2 minutes
- 10 schemas with 100 tables: ~5 minutes

**Search speed:**
- Query time: <0.2 seconds
- Scales with total tables indexed
