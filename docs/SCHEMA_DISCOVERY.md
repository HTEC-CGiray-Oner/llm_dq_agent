# Automatic Schema Discovery & Table Selection

## Overview

The system now automatically:
1. **Discovers** all tables in your Snowflake database
2. **Indexes** table metadata (columns, types, row counts, descriptions)
3. **Embeds** metadata using AI (384-dimensional vectors)
4. **Finds** the right table from natural language queries
5. **Executes** data quality checks on the correct table

**No more manual table specification!**

---

## Architecture

```
User Query: "Check for duplicate customer records"
     ↓
Schema Indexer (RAG)
  - Searches 384-dim embeddings
  - Returns top 3 relevant tables
     ↓
Agent receives context:
  1. CUSTOMERS (95% relevance)
     - Columns: CUST_ID, NAME, EMAIL, PHONE
     - 10,000 rows
  2. CUSTOMER_ORDERS (60% relevance)
  3. CUSTOMER_CONTACTS (55% relevance)
     ↓
Agent selects: CUSTOMERS
     ↓
Executes: check_dataset_duplicates('AGENT_LLM_READ.PUBLIC.CUSTOMERS')
     ↓
Returns results
```

---

## Quick Start

### 1. Build Schema Index (One-Time Setup)

```python
from src.retrieval.schema_indexer import build_schema_index_for_snowflake

# Index all tables in PUBLIC schema
build_schema_index_for_snowflake(
    schema='PUBLIC',
    include_sample=False,  # Set True to include sample data
    max_tables=None        # None = all tables
)
```

**What happens:**
- Queries `INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS`
- Extracts: table names, column names, data types, row counts, comments
- Creates metadata documents for each table
- Embeds documents using `all-MiniLM-L6-v2` model
- Stores in ChromaDB (`./chroma_db/database_schemas` collection)

### 2. Search for Tables

```python
from src.retrieval.schema_indexer import SchemaIndexer

indexer = SchemaIndexer('snowflake')

# Natural language search
results = indexer.search_tables("customer contact information", top_k=3)

for table in results:
    print(f"{table['full_name']} - {table['relevance_score']:.2%} relevant")
```

### 3. Run Smart DQ Check

```python
from src.agent.smart_planner import run_smart_dq_check

# Agent automatically finds the right table!
result = run_smart_dq_check("Check for duplicate customer records")
print(result['output'])
```

---

## Detailed Workflow

### Phase 1: Schema Discovery

**File:** `src/connectors/schema_discovery.py`

```python
discovery = SchemaDiscovery('snowflake')

# Discover all tables
tables = discovery.discover_snowflake_tables(
    database='AGENT_LLM_READ',
    schema='PUBLIC'
)

# Get detailed column info
columns = discovery.get_table_columns('CUSTOMERS')

# Create searchable metadata document
metadata = discovery.create_table_metadata_document(
    'CUSTOMERS',
    include_sample=False
)
```

**Output Example:**
```
TABLE: AGENT_LLM_READ.PUBLIC.CUSTOMERS
Type: BASE TABLE
Row Count: 10,000

COLUMNS:
  - CUST_ID (NUMBER) - Unique customer identifier
  - FIRST_NAME (VARCHAR) - Customer first name
  - LAST_NAME (VARCHAR) - Customer last name
  - EMAIL (VARCHAR) - Email address
  - PHONE (VARCHAR) - Phone number
  - CREATED_DATE (TIMESTAMP) - Account creation date

Created: 2024-01-15
Last Modified: 2024-11-10
```

### Phase 2: Indexing

**File:** `src/retrieval/schema_indexer.py`

```python
indexer = SchemaIndexer('snowflake')

indexer.build_schema_index(
    schema='PUBLIC',
    recreate=True  # Delete old index
)
```

**What's stored:**
- **Text**: Full metadata document (shown above)
- **Embedding**: 384-dimensional vector representing semantic meaning
- **Metadata**: `{'table_name': 'CUSTOMERS', 'full_name': 'AGENT_LLM_READ.PUBLIC.CUSTOMERS'}`

### Phase 3: RAG Search

**When user asks:** "Find duplicate customers"

1. **Embed query**: "Find duplicate customers" → `[0.12, -0.45, 0.89, ...]` (384 dims)
2. **Search ChromaDB**: Find tables with similar embeddings
3. **Return ranked results**:
   ```python
   [
       {'table_name': 'CUSTOMERS', 'relevance_score': 0.95},
       {'table_name': 'CUSTOMER_ORDERS', 'relevance_score': 0.68},
       {'table_name': 'USER_ACCOUNTS', 'relevance_score': 0.62}
   ]
   ```

### Phase 4: Agent Execution

**File:** `src/agent/smart_planner.py`

```python
# Agent receives:
# 1. User query
# 2. Top 3 relevant tables with full metadata
# 3. Tools available (check_dataset_duplicates)

# Agent thinks:
# "CUSTOMERS table has columns for customer info, highest relevance"
# "I should check AGENT_LLM_READ.PUBLIC.CUSTOMERS"

# Agent calls:
check_dataset_duplicates('AGENT_LLM_READ.PUBLIC.CUSTOMERS', 'snowflake')
```

---

## Configuration

### Customize What's Indexed

```python
indexer = SchemaIndexer('snowflake')

indexer.build_schema_index(
    database='AGENT_LLM_READ',      # Specific database
    schema='PUBLIC',                 # Specific schema
    include_sample=True,             # Include sample rows
    max_tables=50,                   # Limit for testing
    recreate=True                    # Fresh index
)
```

### Include Sample Data

```python
# Adds sample rows to metadata for better matching
metadata = discovery.create_table_metadata_document(
    'CUSTOMERS',
    include_sample=True  # Adds 3-5 sample rows
)
```

**Benefit:** Agent can see actual data patterns

---

## Advanced Features

### 1. Multi-Schema Indexing

```python
# Index multiple schemas
for schema in ['PUBLIC', 'ANALYTICS', 'STAGING']:
    indexer.build_schema_index(
        schema=schema,
        recreate=False  # Append to existing index
    )
```

### 2. Periodic Re-indexing

```python
# Re-index when schema changes
import schedule

def rebuild_index():
    build_schema_index_for_snowflake(schema='PUBLIC')

# Run daily at 2 AM
schedule.every().day.at("02:00").do(rebuild_index)
```

### 3. Custom Relevance Tuning

```python
# Adjust number of tables returned
results = indexer.search_tables(
    "customer data",
    top_k=5  # Return top 5 matches
)
```

---

## Examples

### Example 1: Customer Data

```python
query = "Check for duplicate customer records"

# System finds:
# 1. CUSTOMERS (95% relevant)
#    - Has CUST_ID, NAME, EMAIL, PHONE
# 2. CUSTOMER_ADDRESSES (70% relevant)
# 3. CUSTOMER_ORDERS (65% relevant)

# Agent selects CUSTOMERS and runs check
```

### Example 2: Sales Data

```python
query = "Find duplicates in sales transactions"

# System finds:
# 1. SALES_TRANSACTIONS (98% relevant)
#    - Has TRANS_ID, PRODUCT_ID, AMOUNT, DATE
# 2. ORDER_LINE_ITEMS (75% relevant)
# 3. INVOICES (68% relevant)

# Agent selects SALES_TRANSACTIONS
```

### Example 3: Ambiguous Query

```python
query = "Check for duplicates"  # Generic

# System returns multiple candidates with relevance scores
# Agent might ask for clarification or pick highest scored table
```

---

## Maintenance

### When to Rebuild Index

- **New tables added** to database
- **Columns changed** (renamed, added, removed)
- **Table comments updated** (descriptions changed)
- **Initial setup** (first time)

### Quick Rebuild

```bash
# From terminal
poetry run python src/retrieval/schema_indexer.py

# Or from Python
from src.retrieval.schema_indexer import build_schema_index_for_snowflake
build_schema_index_for_snowflake()
```

---

## Performance

### Indexing Speed

- **10 tables**: ~30 seconds
- **50 tables**: ~2 minutes
- **100 tables**: ~4 minutes

*Depends on network speed and table complexity*

### Search Speed

- **Embedding query**: ~0.1 seconds
- **Vector search**: ~0.05 seconds
- **Total**: <0.2 seconds

*Searches are fast!*

---

## Troubleshooting

### "No tables found"

```python
# Check if index exists
from src.retrieval.schema_indexer import SchemaIndexer

indexer = SchemaIndexer('snowflake')
results = indexer.search_tables("test", top_k=1)

if not results:
    print("Index not built. Run: build_schema_index_for_snowflake()")
```

### "Connection error"

- Verify Snowflake credentials in `.env`
- Check database/schema names are correct
- Ensure user has `SELECT` on `INFORMATION_SCHEMA`

### "Poor relevance scores"

- **Add table comments** in Snowflake (improves matching)
- **Include sample data** (`include_sample=True`)
- **Check column names** (descriptive names help)

---

## Complete Example

```bash
# Terminal
poetry run python examples/schema_discovery_demo.py
```

Choose option 4 for full demo!

---

## Benefits

✅ **No manual table specification** - Just ask in natural language
✅ **Automatic discovery** - System finds all tables
✅ **Semantic search** - Matches by meaning, not just keywords
✅ **Scalable** - Works with hundreds of tables
✅ **Self-documenting** - Index contains table metadata
✅ **Privacy-friendly** - Metadata stays in your vector DB

---

## Next Steps

1. **Run the demo**: `poetry run python examples/schema_discovery_demo.py`
2. **Build your index**: Index your Snowflake schema
3. **Try queries**: Ask natural language questions
4. **Customize**: Adjust schemas, add comments, tune relevance
