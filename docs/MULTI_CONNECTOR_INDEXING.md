# Multi-Connector Support: Distinguishing Between Data Sources

## Overview

The indexer now includes connector type information in the metadata, allowing the agent to distinguish between tables from different data sources (Snowflake, PostgreSQL, CSV, etc.).

## What Changed

### 1. Metadata Document Enhancement

Each table's metadata now explicitly shows its data source at the top:

```
DATA SOURCE: SNOWFLAKE
TABLE: AGENT_LLM_READ.TRYOUTS.PRODUCTS
Type: BASE TABLE
...
```

This makes the connector type part of the searchable text, helping the agent understand which system a table belongs to.

### 2. Agent Prompt Update

The agent now understands multiple data sources and can match user queries to the correct connector:

**Query Examples:**
- "Check duplicates in Snowflake products table" → Selects SNOWFLAKE tables
- "Check duplicates in PostgreSQL products table" → Selects POSTGRES tables
- "Find duplicates in CSV customer file" → Selects CSV files

### 3. Enhanced Display

Results now show connector type prominently:

```
Found 3 relevant table(s):

  1. [SNOWFLAKE] AGENT_LLM_READ.TRYOUTS.PRODUCTS
     Relevance: 89.2%

  2. [POSTGRES] PUBLIC.PRODUCTS
     Relevance: 85.7%
```

## How to Use

### Option 1: Index Single Connector

```python
from src.retrieval.schema_indexer import SchemaIndexer

# Index Snowflake only
indexer = SchemaIndexer(connector_type='snowflake')
indexer.build_schema_index(recreate=True)
```

### Option 2: Index Multiple Connectors

```python
from src.retrieval.schema_indexer import SchemaIndexer

# Index Snowflake first
snowflake_indexer = SchemaIndexer(connector_type='snowflake')
snowflake_indexer.build_schema_index(recreate=True)

# Then append PostgreSQL
postgres_indexer = SchemaIndexer(connector_type='postgres')
postgres_indexer.build_schema_index(recreate=False)  # Append!
```

### Option 3: Use Helper Script

```bash
poetry run python examples/index_multi_connector.py
```

Interactive menu to index:
- Snowflake only
- PostgreSQL only
- CSV only
- Snowflake + PostgreSQL
- All three

## Query Examples

### Specify Connector in Query

```python
from src.agent.smart_planner import run_smart_dq_check

# Query Snowflake specifically
result = run_smart_dq_check("how many duplicate rows are in the snowflake products table?")

# Query PostgreSQL specifically
result = run_smart_dq_check("how many duplicate rows are in the postgres products table?")

# Query CSV specifically
result = run_smart_dq_check("check for duplicates in the CSV customer file")
```

### Generic Query (Searches All)

```python
# Without specifying connector - agent will consider all indexed tables
result = run_smart_dq_check("check for duplicate products")
```

## How It Works

### Indexing Flow

```
1. Create SchemaIndexer with connector_type='snowflake'
   ↓
2. Discover tables from Snowflake
   ↓
3. For each table, create metadata document:
   - Add "DATA SOURCE: SNOWFLAKE" at top
   - Include table info, columns, samples
   ↓
4. Store in vector DB with metadata:
   {
     'table_name': 'PRODUCTS',
     'full_name': 'DB.SCHEMA.PRODUCTS',
     'connector_type': 'snowflake'  ← Used by DQ checks
   }
```

### Query Flow

```
User: "Check duplicates in Snowflake products table"
   ↓
Vector Search: Searches for "Snowflake products"
   - Finds tables with "SNOWFLAKE" in metadata
   - Ranks by relevance
   ↓
Agent Receives Context:
   --- Table #1 ---
   Connector Type: SNOWFLAKE
   Full Name: AGENT_LLM_READ.TRYOUTS.PRODUCTS
   Metadata:
   DATA SOURCE: SNOWFLAKE
   TABLE: AGENT_LLM_READ.TRYOUTS.PRODUCTS
   ...
   ↓
Agent Decision:
   "User asked for Snowflake table, this is SNOWFLAKE connector"
   → Selects this table
   ↓
DQ Check:
   Uses connector_type='snowflake' from metadata
   Runs check on correct database
```

## Key Benefits

### 1. Natural Language Distinction
Users can specify data source naturally:
- "Snowflake", "snowflake", "SNOWFLAKE" all work
- "PostgreSQL", "Postgres", "postgres", "pg" all work
- "CSV", "csv file", "CSV file" all work

### 2. Accurate Table Selection
Agent understands context:
```python
# Query 1: "Check Snowflake products"
# → Agent selects SNOWFLAKE table

# Query 2: "Check Postgres products"
# → Agent selects POSTGRES table (different products table!)
```

### 3. Single Agent, Multiple Sources
One agent can work with multiple databases:
- Development: PostgreSQL
- Production: Snowflake
- Archives: CSV files

### 4. Automatic Connector Selection
The DQ check automatically uses the right connector based on the selected table's metadata.

## Configuration

Each connector has its own discovery settings in `config/settings.yaml`:

```yaml
connectors:
  snowflake:
    discovery:
      auto_discover_schemas: true
      include_sample: true

  postgres:
    discovery:
      schema: public
      include_sample: true

  csv:
    base_path: ./data
```

## Testing

Run the demo to test multi-connector queries:

```bash
# 1. Index both connectors
poetry run python examples/index_multi_connector.py
# Choose option 4: Snowflake + PostgreSQL

# 2. Run demo with both queries
poetry run python examples/schema_discovery_demo.py
# Choose option 3: Run automatic DQ check
```

The two queries in the demo show the distinction:
```python
"how many duplicate rows are in the snowflake products table?"
"how many duplicate rows are in the postgres products table?"
```

Agent will correctly identify and use different tables based on the connector mentioned.

## Troubleshooting

### "Agent selects wrong connector"

**Issue:** Query says "Snowflake" but agent selects PostgreSQL table

**Solution:** Rebuild index with `recreate=True` to ensure metadata includes "DATA SOURCE" field:
```python
indexer = SchemaIndexer(connector_type='snowflake')
indexer.build_schema_index(recreate=True)
```

### "No tables found for connector"

**Issue:** Query for PostgreSQL but no results

**Solution:** Make sure you indexed that connector:
```python
# Check what's in the index
from src.retrieval.schema_indexer import SchemaIndexer
indexer = SchemaIndexer()
results = indexer.search_tables("*", top_k=10)
for r in results:
    print(f"{r['connector_type']}: {r['full_name']}")
```

### "Both connectors show same tables"

**Issue:** Snowflake and Postgres queries return same table

**Solution:** This is normal if you only indexed one connector. Index both:
```bash
poetry run python examples/index_multi_connector.py
# Choose option 4 or 5
```

## Example Session

```bash
$ poetry run python examples/index_multi_connector.py

MULTI-CONNECTOR INDEXING

Which connectors do you want to index?
1. Snowflake only
2. PostgreSQL only
3. CSV only
4. Snowflake + PostgreSQL
5. All three

Enter choice: 4

INDEXING SNOWFLAKE TABLES
Auto-discovered 2 schemas: PUBLIC, TRYOUTS
[1/2] INDEXING SCHEMA: PUBLIC
✓ Indexed 1 tables from schema PUBLIC

[2/2] INDEXING SCHEMA: TRYOUTS
✓ Indexed 1 tables from schema TRYOUTS

INDEXING POSTGRESQL TABLES
Indexing single schema: public
✓ Indexed 5 tables from schema public

✓ INDEXING COMPLETE

$ poetry run python examples/schema_discovery_demo.py

Query: "Check duplicates in Snowflake products table"

Found 2 relevant table(s):

  1. [SNOWFLAKE] AGENT_LLM_READ.TRYOUTS.PRODUCTS
     Relevance: 92.3%

  2. [POSTGRES] PUBLIC.PRODUCTS
     Relevance: 88.1%

Agent selected: AGENT_LLM_READ.TRYOUTS.PRODUCTS
✓ Found 2 duplicate rows in Snowflake table
```

## See Also

- **[Schema Discovery](SCHEMA_DISCOVERY.md)** - How schema discovery works
- **[Connectors Guide](CONNECTORS.md)** - Connector configuration
- **[Configuration](CONFIGURATION.md)** - All settings
