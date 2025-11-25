# Schema Indexing Flow Diagram

## Complete Flow with Class/Method Names

```mermaid
graph TD
    A[SchemaIndexer.__init__] --> B[SchemaIndexer._load_discovery_config]
    B --> C[SchemaIndexer.build_schema_index]

    C --> D{auto_discover_schemas?}
    D -->|Yes| E[SchemaIndexer._get_all_schemas]
    D -->|No| F{schemas provided?}
    F -->|Yes| G[Use provided schemas list]
    F -->|No| H[Use None - current schema]

    E --> K[ConnectorFactory.create_connector]
    G --> K
    H --> K
    K --> L[SnowflakeConnector.__init__ OR PostgresConnector.__init__]
    L --> M[BaseConnector.__enter__]
    M --> N[SnowflakeConnector.connect OR PostgresConnector.connect]

    N --> O{database from config?}
    O -->|Use config database| R[Use database from settings.yaml/.env]
    O -->|Override/None| P[cursor.execute 'SELECT CURRENT_DATABASE()']
    O -->|Override/None| Q[cursor.execute 'SELECT current_database()']

    P --> S[cursor.execute INFORMATION_SCHEMA query - SchemaIndexer._get_all_schemas]
    Q --> S
    R --> S

    S --> T[Filter excluded schemas - SchemaIndexer._get_all_schemas]
    T --> U[For each schema: LOOP]

    U --> V[SchemaDiscovery.discover_all_table_metadata]
    V --> W[SchemaDiscovery.discover_tables - router method]
    W --> X[SchemaDiscovery.discover_snowflake_tables OR discover_postgres_tables]
    X --> Y[Return to discover_all_table_metadata - process each table]

    Y --> Z[SchemaDiscovery.create_table_metadata_document]
    Z --> AA[SchemaDiscovery.get_table_columns]
    AA --> BB[SchemaDiscovery._get_snowflake_columns OR _get_postgres_columns]
    BB --> CC[SchemaDiscovery.get_table_sample - if include_sample=True]

    CC --> DD[Return metadata_docs list to SchemaIndexer - All methods Y→Z→AA→BB→CC cumulate data in discover_all_table_metadata]

    DD --> EE[chromadb.PersistentClient - SchemaIndexer.build_schema_index]
    EE --> FF{recreate=True?}
    FF -->|Yes| GG[client.delete_collection]
    FF -->|No| HH[Skip deletion]

    GG --> II[HuggingFaceEmbeddings.embed_documents - Called by Chroma.from_texts in SchemaIndexer.build_schema_index]
    HH --> II

    II --> JJ[Chroma.from_texts - SchemaIndexer.build_schema_index]
    JJ --> KK[Success Message]

    KK --> LL{More schemas?}
    LL -->|Yes| U
    LL -->|No| MM[Build Complete]
```

## Detailed Method Flow by Phase

### Phase 1: Initialization
```
SchemaIndexer.__init__
├── SchemaDiscovery.__init__
├── HuggingFaceEmbeddings.__init__
└── SchemaIndexer._load_discovery_config
    ├── yaml.safe_load (settings.yaml)
    └── setattr (config attributes)
```

### Phase 2: Schema Discovery
```
SchemaIndexer.build_schema_index
├── SchemaIndexer._get_all_schemas
│   ├── ConnectorFactory.create_connector
│   │   └── ConnectorFactory._load_config
│   │       ├── yaml.safe_load (settings.yaml)
│   │       └── os.getenv (.env override)
│   ├── BaseConnector.__enter__
│   │   └── SnowflakeConnector.connect OR PostgresConnector.connect
│   ├── cursor.execute("SELECT CURRENT_DATABASE()" OR "SELECT current_database()")
│   ├── cursor.execute(schema discovery query)
│   └── Filter with self.exclude_schemas
└── Schema selection logic
```

### Phase 3: Table Discovery (Per Schema)
```
SchemaDiscovery.discover_all_table_metadata
├── SchemaDiscovery.discover_tables (router method)
│   └── SchemaDiscovery.discover_snowflake_tables OR discover_postgres_tables
│       ├── cursor.execute(table discovery query)
│       └── Return complete table list
└── For each table in table list:
    └── SchemaDiscovery.create_table_metadata_document
        ├── SchemaDiscovery.get_table_columns
        │   └── SchemaDiscovery._get_snowflake_columns OR _get_postgres_columns
        ├── SchemaDiscovery.get_table_sample (if include_sample=True)
        └── Format metadata text and cumulate in metadata_docs list
```

### Phase 4: Vector Indexing
```
SchemaIndexer.build_schema_index (continued)
├── chromadb.PersistentClient(path="./chroma_db")
├── client.delete_collection("database_schemas") (if recreate=True)
├── Chroma.from_texts
│   ├── HuggingFaceEmbeddings.embed_documents (called internally)
│   ├── chromadb.Collection.add
│   └── Store vectors + metadata
└── Print success metrics
```

## Key Decision Points

### Database Detection:
```
# Most common case: database comes from .env/settings.yaml
if database from config:
    └── Use SNOWFLAKE_DATABASE or POSTGRES_DATABASE from .env
# Fallback case: no config or explicit None override
else:
    ├── Snowflake: cursor.execute("SELECT CURRENT_DATABASE()")
    └── PostgreSQL: cursor.execute("SELECT current_database()")
```

### Schema Selection:
```
if auto_discover_schemas:
    └── SchemaIndexer._get_all_schemas()
elif schemas provided:
    └── Use explicit schemas list
else:
    └── Use None (current schema)
```

### Collection Management:
```
if recreate and i == 1:
    └── client.delete_collection()
else:
    └── Append to existing collection
```

## Environment Variable Integration Points

```
ConnectorFactory._load_config
├── SNOWFLAKE_DATABASE → config['database']
├── SNOWFLAKE_SCHEMA → config['schema']
├── POSTGRES_DATABASE → config['database']
└── Other connection params...
```

## Error Handling Methods

```
try-catch blocks in:
├── SchemaIndexer._load_discovery_config (YAML loading)
├── ConnectorFactory._load_config (Config loading)
├── SchemaIndexer.build_schema_index (Collection deletion)
└── All database operations (Connection errors)
```

This flow shows exactly which class methods are called at each step of the indexing process, making it easy to trace execution and debug issues.
