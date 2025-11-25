# Schema Indexing Process Documentation

## Overview

The schema indexing system creates a searchable vector database of table metadata to enable intelligent table discovery in RAG (Retrieval-Augmented Generation) applications. The process systematically discovers database schemas, extracts table metadata, and converts this information into searchable vector embeddings using a four-phase approach.

## Architecture & Design Patterns

The indexing system implements a robust database abstraction layer using the **Factory design pattern** and **abstract base classes** to enable seamless multi-database support:

- **`base_connector.py`**: Defines the abstract `BaseConnector` class that establishes the contract for all database interactions
- **`connector_factory.py`**: Implements the Factory pattern to dynamically instantiate appropriate connectors (`SnowflakeConnector` or `PostgresConnector`) based on configuration
- **Database-specific connectors**: `snowflake_connector.py` and `postgres_connector.py` inherit from `BaseConnector` and provide database-specific connection logic, schema discovery methods, and metadata extraction capabilities
- **Configuration management**: Hierarchical configuration through `settings.yaml` and environment variables (`.env` files)
- **Resource management**: Each connector implements Python's context manager protocol (`__enter__` and `__exit__`) for automatic resource cleanup

This design ensures extensibility for future database types while guaranteeing consistent behavior across all implementations.

## Configuration Requirements

### Discovery Configuration
The indexing process is controlled through `config/settings.yaml` with the following structure:

```yaml
connectors:
  snowflake:
    discovery:
      database: PROD_SALES          # Target database (required)
      schema: null                  # Specific schemas list or null for auto-discovery
      auto_discover_schemas: true   # Enable automatic schema discovery
      exclude_schemas: []           # Schemas to exclude from indexing
      include_sample: true          # Include table data samples
      sample_row_limit: 3          # Number of sample rows to include
```

### Discovery Strategy
- **Auto-discovery** (`auto_discover_schemas: true`): Automatically discovers all available schemas in the specified database
- **Manual selection** (`auto_discover_schemas: false`): Uses explicitly defined schema list
- **Current schema only**: When no schemas are specified and auto-discovery is disabled

### Schema Selection Priority
When `auto_discover_schemas: false` and no explicit schema list is provided, the system uses the **default schema** with the following priority order:

1. **Environment Variable**:
   - Snowflake: `SNOWFLAKE_SCHEMA` from `.env` file
   - Postgres: `POSTGRES_SCHEMA` from `.env` file
2. **Configuration File**: `schema` value from `settings.yaml`
3. **Database Default**:
   - Snowflake: User's default schema (typically `PUBLIC`)
   - Postgres: `public` schema

Example environment configuration:
```bash
# .env file
SNOWFLAKE_SCHEMA=ANALYTICS
POSTGRES_SCHEMA=reporting
```

Example YAML configuration:
```yaml
connectors:
  snowflake:
    schema: ANALYTICS              # Explicit schema selection
    discovery:
      auto_discover_schemas: false # Disable auto-discovery
```

## Four-Phase Execution Process

### Phase 1: Initialization
```
SchemaIndexer.__init__
├── SchemaDiscovery.__init__               # Initialize schema discovery component
├── HuggingFaceEmbeddings.__init__        # Initialize embedding model (all-MiniLM-L6-v2)
└── SchemaIndexer._load_discovery_config   # Load configuration from settings.yaml
    ├── yaml.safe_load(settings.yaml)
    └── setattr(config attributes)
```

**Purpose**: Establishes the foundation by loading configuration values, initializing the embedding model, and preparing the schema discovery component.

### Phase 2: Schema Discovery
```
SchemaIndexer.build_schema_index
├── SchemaIndexer._get_all_schemas
│   ├── ConnectorFactory.create_connector         # Factory pattern implementation
│   │   └── ConnectorFactory._load_config
│   │       ├── yaml.safe_load(settings.yaml)    # Load base configuration
│   │       └── os.getenv(.env override)          # Apply environment overrides
│   ├── BaseConnector.__enter__                   # Context manager entry
│   │   └── SnowflakeConnector.connect OR PostgresConnector.connect
│   ├── cursor.execute("SELECT CURRENT_DATABASE()" OR "SELECT current_database()")
│   ├── cursor.execute(schema discovery query)    # Database-specific schema queries
│   └── Filter with self.exclude_schemas          # Apply exclusion filters
└── Schema selection logic
```

**Purpose**: Establishes database connections using the factory pattern, discovers available schemas based on configuration, and applies filtering rules.

### Phase 3: Table Discovery (Per Schema)
```
SchemaDiscovery.discover_all_table_metadata
├── SchemaDiscovery.discover_tables (router method)
│   └── SchemaDiscovery.discover_snowflake_tables OR discover_postgres_tables
│       ├── cursor.execute(table discovery query)  # Database-specific table queries
│       └── Return complete table list
└── For each table in table list:
    └── SchemaDiscovery.create_table_metadata_document
        ├── SchemaDiscovery.get_table_columns
        │   └── SchemaDiscovery._get_snowflake_columns OR _get_postgres_columns
        ├── SchemaDiscovery.get_table_sample (if include_sample=True)
        └── Format metadata text and cumulate in metadata_docs list
```

**Purpose**: Systematically discovers all tables within each schema, extracts comprehensive metadata (columns, data types, constraints), and formats this information into searchable text documents.

**Metadata Extraction Includes**:
- Table structure (column names, data types, nullable constraints)
- Table location (database.schema.table hierarchy)
- Optional data samples for context
- Row count estimates
- Formatted descriptions for semantic search

### Phase 4: Vector Indexing
```
SchemaIndexer.build_schema_index (continued)
├── chromadb.PersistentClient(path="./chroma_db")           # Initialize ChromaDB client
├── client.delete_collection("database_schemas") (if recreate=True)  # Optional cleanup
├── Chroma.from_texts                                       # Create vector store
│   ├── HuggingFaceEmbeddings.embed_documents (called internally from Phase 3)
│   ├── chromadb.Collection.add                           # Store vectors + metadata
│   └── Store vectors + metadata
└── Print success metrics
```

**Purpose**: Converts the metadata documents from Phase 3 into 384-dimensional vector embeddings using the HuggingFace `all-MiniLM-L6-v2` model and stores them in ChromaDB for semantic search.

**Storage Components**:
- **Documents**: Searchable text representations of table metadata
- **Embeddings**: 384-dimensional vectors for semantic similarity
- **Metadata**: Structured information for filtering and context
- **IDs**: Unique identifiers for each table

## Data Flow Between Phases

### **Phase 1 → Phase 2**: Configuration Parameters and Initialized Components
```python
# Phase 1 Output: Initialized indexer with configuration 📍 SchemaIndexer.__init__() schema_indexer.py:23-40
schema_indexer = SchemaIndexer(
    embedding_model=HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2"),
    config={
        'auto_discover_schemas': True,
        'exclude_schemas': ['INFORMATION_SCHEMA'],
        'include_sample': True,
        'sample_row_limit': 3
    }
)

# Phase 2 Input: Ready indexer with loaded discovery configuration
indexer.build_schema_index()
```

### **Phase 2 → Phase 3**: Discovered Schema List and Database Connections
```python
# Phase 2 Output: Available schemas with active connections 📍 SchemaIndexer._get_all_schemas() schema_indexer.py:65-85
discovered_schemas = [
    {
        'database': 'PROD_SALES',
        'schema': 'PUBLIC',
        'connector': active_snowflake_connector
    },
    {
        'database': 'PROD_SALES',
        'schema': 'ANALYTICS',
        'connector': active_snowflake_connector
    }
]

# Phase 3 Input: Schema list for table discovery iteration
for schema_info in discovered_schemas:
    discover_all_table_metadata(schema_info)
```

### **Phase 3 → Phase 4**: Metadata Documents List Ready for Vectorization
```python
# Phase 3 Output: Formatted metadata documents 📍 SchemaDiscovery.create_table_metadata_document() schema_discovery.py:150-200
metadata_docs = [
    "Database: PROD_SALES\nSchema: PUBLIC\nTable: CUSTOMERS\nColumns: customer_id (NUMBER), name (VARCHAR), email (VARCHAR)...",
    "Database: PROD_SALES\nSchema: PUBLIC\nTable: ORDERS\nColumns: order_id (NUMBER), customer_id (NUMBER), amount (NUMBER)...",
    "Database: PROD_SALES\nSchema: ANALYTICS\nTable: SALES_SUMMARY\nColumns: date (DATE), total_revenue (NUMBER)..."
]

# Phase 4 Input: Text documents ready for embedding generation
Chroma.from_texts(texts=metadata_docs, embedding=embeddings)
```

### **Phase 4 Output**: Persistent Vector Database Ready for Semantic Search
```python
# Phase 4 Output: Indexed vector database 📍 SchemaIndexer.build_schema_index() schema_indexer.py:120-140
vector_store = {
    'collection_name': 'database_schemas',
    'storage_path': './chroma_db',
    'total_documents': 247,
    'embedding_dimensions': 384,
    'indexed_tables': [
        'PROD_SALES.PUBLIC.CUSTOMERS',
        'PROD_SALES.PUBLIC.ORDERS',
        'PROD_SALES.ANALYTICS.SALES_SUMMARY'
    ]
}

# Ready for RAG queries: semantic search enabled
search_results = vector_store.similarity_search("customer data", k=3)
```

## Key Benefits

### Intelligent Table Discovery
- **Natural Language Queries**: Users can search with phrases like "find sales tables" or "show customer data"
- **Semantic Matching**: Vector embeddings enable context-aware table recommendations
- **Cross-Database Support**: Unified search across multiple database platforms

### Scalable Architecture
- **Factory Pattern**: Easy addition of new database types
- **Configurable Discovery**: Flexible schema and table selection
- **Efficient Storage**: Optimized vector storage for fast retrieval

### Robust Implementation
- **Automatic Resource Management**: Context managers ensure proper cleanup
- **Error Handling**: Graceful degradation for connection failures
- **Configuration Flexibility**: Multiple configuration sources (YAML, environment variables)

## Technical Specifications

- **Embedding Model**: HuggingFace `all-MiniLM-L6-v2` (384 dimensions)
- **Vector Database**: ChromaDB with persistent storage
- **Supported Databases**: Snowflake, Postgres (extensible architecture)
- **Configuration**: YAML + environment variable overrides
- **Storage Location**: `./chroma_db/database_schemas` collection

## Usage

Execute the indexing process manually:
```bash
python src/retrieval/schema_indexer.py
```

The resulting vector index enables the RAG-powered table discovery system to intelligently match user queries with relevant database tables, forming the foundation for automated data quality analysis and reporting.
