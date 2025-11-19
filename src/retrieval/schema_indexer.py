# src/retrieval/schema_indexer.py
"""
Index database schema metadata into the vector database.
Allows the agent to discover and select appropriate tables based on user queries.
"""
from langchain_community.vectorstores import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
import chromadb
from src.connectors.schema_discovery import SchemaDiscovery
from typing import Optional, List
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


SCHEMA_VECTOR_DB_PATH = "./chroma_db"
SCHEMA_COLLECTION_NAME = "database_schemas"


class SchemaIndexer:
    """Index database schema metadata for RAG-based table discovery."""

    def __init__(self, connector_type: str = 'snowflake'):
        self.connector_type = connector_type
        self.discovery = SchemaDiscovery(connector_type)
        self.embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
        self._load_discovery_config()

    def _load_discovery_config(self):
        """Load database and schema from settings.yaml"""
        import yaml
        settings_path = os.path.join(os.path.dirname(__file__), '../../config/settings.yaml')
        self.default_database = None
        self.default_schema = None
        self.default_schemas = None
        self.auto_discover_schemas = False
        self.exclude_schemas = []
        self.default_include_sample = False
        self.default_sample_row_limit = 3

        if os.path.exists(settings_path):
            with open(settings_path, 'r') as f:
                settings = yaml.safe_load(f)
                connector_config = settings.get('connectors', {}).get(self.connector_type, {})
                discovery_config = connector_config.get('discovery', {}) if connector_config else {}

                # Get discovery settings with safe defaults
                self.default_database = discovery_config.get('database') if discovery_config else None
                self.default_schema = discovery_config.get('schema') if discovery_config else None
                self.default_schemas = discovery_config.get('schemas') if discovery_config else None
                self.auto_discover_schemas = discovery_config.get('auto_discover_schemas', False) if discovery_config else False
                self.exclude_schemas = discovery_config.get('exclude_schemas', []) if discovery_config else []
                self.default_include_sample = discovery_config.get('include_sample', False) if discovery_config else False
                self.default_sample_row_limit = discovery_config.get('sample_row_limit', 3) if discovery_config else 3

    def _get_all_schemas(self, database: Optional[str] = None) -> List[str]:
        """Discover all schemas from INFORMATION_SCHEMA."""
        from src.connectors.connector_factory import ConnectorFactory

        connector = ConnectorFactory.create_connector(self.connector_type)

        with connector:
            cursor = connector._cursor

            if self.connector_type == 'snowflake':
                # Snowflake schema discovery
                # Get current database if not specified
                if database is None or database == '':
                    cursor.execute("SELECT CURRENT_DATABASE()")
                    database = cursor.fetchone()[0]

                query = f"""
                SELECT SCHEMA_NAME
                FROM {database}.INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
                ORDER BY SCHEMA_NAME
                """
                cursor.execute(query)
                schemas = [row[0] for row in cursor.fetchall()]

            elif self.connector_type == 'postgres':
                # PostgreSQL schema discovery
                query = """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
                    AND schema_name NOT LIKE 'pg_temp_%'
                    AND schema_name NOT LIKE 'pg_toast_temp_%'
                ORDER BY schema_name
                """
                cursor.execute(query)
                schemas = [row[0] for row in cursor.fetchall()]

            else:
                raise NotImplementedError(f"Schema discovery not implemented for {self.connector_type}")

            # Filter out excluded schemas
            schemas = [s for s in schemas if s not in self.exclude_schemas]

            return schemas

    def build_schema_index(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        schemas: Optional[List[str]] = None,
        auto_discover_schemas: Optional[bool] = None,
        include_sample: Optional[bool] = None,
        sample_row_limit: Optional[int] = None,
        max_tables: Optional[int] = None,
        recreate: bool = True
    ):
        """
        Discover all tables and index their metadata.

        Args:
            database: Database to scan (None = use config or current database)
            schema: Single schema to scan (None = use config)
            schemas: List of specific schemas to scan
            auto_discover_schemas: If True, automatically discover and index ALL schemas
            include_sample: Include sample data (None = use config, default: from settings.yaml)
            sample_row_limit: Number of sample rows per table (None = use config)
            max_tables: Limit number of tables per schema
            recreate: If True, delete existing collection first
        """
        # Use config defaults if not specified
        include_sample = include_sample if include_sample is not None else self.default_include_sample
        sample_row_limit = sample_row_limit or self.default_sample_row_limit
        # Use config defaults if not specified
        database = database or self.default_database

        # Determine which schemas to index
        if auto_discover_schemas or (auto_discover_schemas is None and self.auto_discover_schemas):
            # Auto-discover all schemas
            schemas_to_index = self._get_all_schemas(database)
            print(f"Auto-discovered {len(schemas_to_index)} schemas: {', '.join(schemas_to_index)}")
        elif schemas or self.default_schemas:
            # Use provided list or config list
            schemas_to_index = schemas or self.default_schemas
            print(f"Indexing {len(schemas_to_index)} specified schemas: {', '.join(schemas_to_index)}")
        elif schema or self.default_schema:
            # Single schema
            schemas_to_index = [schema or self.default_schema]
            print(f"Indexing single schema: {schemas_to_index[0]}")
        else:
            # No schema specified, will use connection default
            schemas_to_index = [None]

        # Index all schemas
        total_tables = 0
        for i, schema_name in enumerate(schemas_to_index, 1):
            schema_display = schema_name or "(current schema)"
            print(f"\n{'='*70}")
            print(f"[{i}/{len(schemas_to_index)}] INDEXING SCHEMA: {schema_display}")
            print(f"{'='*70}")

            # Discover all table metadata for this schema
            metadata_docs = self.discovery.discover_all_table_metadata(
                database=database,
                schema=schema_name,
                include_sample=include_sample,
                sample_row_limit=sample_row_limit,
                max_tables=max_tables
            )

            if not metadata_docs:
                print(f"No tables found in schema {schema_display}")
                continue

            # Prepare data for indexing
            texts = [doc['metadata'] for doc in metadata_docs]
            metadatas = [
                {
                    'table_name': doc['table_name'],
                    'full_name': doc['full_name'],
                    'connector_type': self.connector_type,
                    'schema': schema_name or 'default'
                }
                for doc in metadata_docs
            ]

            # Create ChromaDB client
            client = chromadb.PersistentClient(path=SCHEMA_VECTOR_DB_PATH)

            # Delete existing collection if recreate=True and first schema
            if recreate and i == 1:
                try:
                    client.delete_collection(SCHEMA_COLLECTION_NAME)
                    print(f"✓ Deleted existing collection: {SCHEMA_COLLECTION_NAME}")
                except:
                    pass

            # Create vector store
            print(f"\nIndexing {len(texts)} table metadata documents...")
            vectorstore = Chroma.from_texts(
                texts=texts,
                embedding=self.embeddings,
                metadatas=metadatas,
                client=client,
                collection_name=SCHEMA_COLLECTION_NAME
            )

            total_tables += len(texts)
            print(f"✓ Indexed {len(texts)} tables from schema {schema_display}")

        print(f"\n{'='*70}")
        print(f"✓ SCHEMA INDEX BUILT SUCCESSFULLY")
        print(f"  - Collection: {SCHEMA_COLLECTION_NAME}")
        print(f"  - Schemas indexed: {len(schemas_to_index)}")
        print(f"  - Total tables indexed: {total_tables}")
        print(f"  - Path: {SCHEMA_VECTOR_DB_PATH}")
        print(f"{'='*70}\n")

    def _get_database_mappings(self) -> dict:
        """
        Build dynamic database name to connector type mapping from indexed metadata.
        This learns the mapping automatically instead of hardcoding.

        Returns:
            Dict mapping database names to connector types
        """
        mappings = {}

        # Connect to vector store
        client = chromadb.PersistentClient(path=SCHEMA_VECTOR_DB_PATH)

        try:
            collection = client.get_collection(SCHEMA_COLLECTION_NAME)

            # Get all metadata to analyze database patterns
            all_results = collection.get(include=['metadatas'])

            if all_results['metadatas']:
                for metadata in all_results['metadatas']:
                    full_name = metadata.get('full_name', '')
                    connector_type = metadata.get('connector_type', '')

                    # Extract database name from full table path
                    if connector_type and full_name:
                        # For PostgreSQL: database.schema.table -> database = database name
                        # For Snowflake: DATABASE.SCHEMA.TABLE -> DATABASE = database name
                        parts = full_name.split('.')
                        if len(parts) >= 2:
                            database_name = parts[0]
                            # Map both exact and partial matches
                            mappings[database_name.lower()] = connector_type

                            # Also map common words in database name
                            db_words = database_name.lower().replace('_', ' ').split()
                            for word in db_words:
                                if len(word) >= 3:  # Avoid very short words
                                    mappings[word] = connector_type

        except Exception as e:
            print(f"Warning: Could not build database mappings: {e}")

        return mappings

    def search_tables(self, query: str, top_k: int = 3) -> List[dict]:
        """
        Search for relevant tables based on a natural language query with intelligent database matching.

        Args:
            query: User's natural language query
            top_k: Number of relevant tables to return

        Returns:
            List of dicts with table info and relevance scores
        """
        # Get dynamic database mappings from indexed metadata
        database_mappings = self._get_database_mappings()

        # Detect database preference from query
        preferred_connector = None
        query_lower = query.lower()

        # Check for explicit database mentions first
        if any(db in query_lower for db in ['postgres', 'postgresql']):
            preferred_connector = 'postgres'
        elif any(db in query_lower for db in ['snowflake']):
            preferred_connector = 'snowflake'
        else:
            # Check for database names/words from actual indexed databases
            query_words = query_lower.replace('_', ' ').split()
            for word in query_words:
                if word in database_mappings:
                    preferred_connector = database_mappings[word]
                    break

            # Also check for partial matches in the full query
            if not preferred_connector:
                for db_name, connector in database_mappings.items():
                    if db_name in query_lower:
                        preferred_connector = connector
                        break

        # Connect to vector store
        client = chromadb.PersistentClient(path=SCHEMA_VECTOR_DB_PATH)

        try:
            collection = client.get_collection(SCHEMA_COLLECTION_NAME)
        except:
            print(f"Schema collection '{SCHEMA_COLLECTION_NAME}' not found. Run build_schema_index() first.")
            return []

        # Perform similarity search with expanded results
        query_embedding = self.embeddings.embed_query(query)
        search_limit = max(top_k * 2, 6)  # Get more results for filtering/boosting
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=search_limit,
            include=['documents', 'metadatas', 'distances']
        )

        # Format and boost results based on database preference
        relevant_tables = []
        if results['documents'] and len(results['documents'][0]) > 0:
            for i, (doc, metadata, distance) in enumerate(zip(
                results['documents'][0],
                results['metadatas'][0],
                results['distances'][0]
            )):
                base_relevance = 1 - distance  # Convert distance to similarity
                boosted_relevance = base_relevance

                # Apply database preference boost
                if preferred_connector and metadata['connector_type'] == preferred_connector:
                    boosted_relevance = min(base_relevance + 0.3, 1.0)  # Cap at 1.0

                # Apply table name matching boost
                table_name = metadata.get('table_name', '').lower()
                query_words = query_lower.split()

                # Boost if table name matches words in query
                table_name_boost = 0
                for word in query_words:
                    if len(word) >= 3 and word in table_name:
                        table_name_boost += 0.2  # Additional boost for table name match

                # Apply table name boost
                boosted_relevance = min(boosted_relevance + table_name_boost, 1.0)

                relevant_tables.append({
                    'rank': i + 1,
                    'table_name': metadata['table_name'],
                    'full_name': metadata['full_name'],
                    'metadata': doc,
                    'relevance_score': boosted_relevance,
                    'connector_type': metadata['connector_type'],
                    'original_relevance': base_relevance,  # Keep original for debugging
                    'boosted': preferred_connector == metadata['connector_type'],
                    'table_boost': table_name_boost,
                    'detected_db': preferred_connector,  # Show what database was detected
                    'db_mappings_used': database_mappings if preferred_connector else None
                })        # Re-sort by boosted relevance and take top_k
        relevant_tables.sort(key=lambda x: x['relevance_score'], reverse=True)
        final_results = relevant_tables[:top_k]

        # Update ranks after re-sorting
        for i, table in enumerate(final_results):
            table['rank'] = i + 1

        return final_results


def build_schema_index_for_snowflake(
    database: Optional[str] = None,
    schema: Optional[str] = None,
    include_sample: Optional[bool] = None,
    sample_row_limit: Optional[int] = None,
    max_tables: Optional[int] = None
):
    """
    Convenience function to build schema index for Snowflake.

    Usage:
        from src.retrieval.schema_indexer import build_schema_index_for_snowflake

        # Use config defaults (sample data enabled by default)
        build_schema_index_for_snowflake()

        # Override to disable samples
        build_schema_index_for_snowflake(include_sample=False)

        # Change sample size
        build_schema_index_for_snowflake(sample_row_limit=5)
    """
    indexer = SchemaIndexer('snowflake')
    indexer.build_schema_index(
        database=database,
        schema=schema,
        include_sample=include_sample,
        sample_row_limit=sample_row_limit,
        max_tables=max_tables
    )


if __name__ == "__main__":
    # Build schema index when run directly
    print("Building schema index for Snowflake...")
    print("Using database and schema from settings.yaml (or current connection defaults)")
    build_schema_index_for_snowflake(
        # Uses config defaults (include_sample and sample_row_limit from settings.yaml)
        max_tables=None  # None = all tables
    )
