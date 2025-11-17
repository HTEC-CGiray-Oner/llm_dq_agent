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

        if os.path.exists(settings_path):
            with open(settings_path, 'r') as f:
                settings = yaml.safe_load(f)
                discovery_config = settings.get('connectors', {}).get(self.connector_type, {}).get('discovery', {})
                self.default_database = discovery_config.get('database')
                self.default_schema = discovery_config.get('schema')
                self.default_schemas = discovery_config.get('schemas')
                self.auto_discover_schemas = discovery_config.get('auto_discover_schemas', False)
                self.exclude_schemas = discovery_config.get('exclude_schemas', [])

    def _get_all_schemas(self, database: Optional[str] = None) -> List[str]:
        """Discover all schemas from INFORMATION_SCHEMA."""
        from src.connectors.connector_factory import ConnectorFactory

        connector = ConnectorFactory.create_connector(self.connector_type)

        with connector:
            cursor = connector._cursor

            # Get current database if not specified
            if not database:
                cursor.execute("SELECT CURRENT_DATABASE()")
                database = cursor.fetchone()[0]

            # Query all schemas
            query = f"""
            SELECT SCHEMA_NAME
            FROM {database}.INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
            ORDER BY SCHEMA_NAME
            """

            cursor.execute(query)
            schemas = [row[0] for row in cursor.fetchall()]

            # Filter out excluded schemas
            schemas = [s for s in schemas if s not in self.exclude_schemas]

            return schemas

    def build_schema_index(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        schemas: Optional[List[str]] = None,
        auto_discover_schemas: Optional[bool] = None,
        include_sample: bool = False,
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
            include_sample: Include sample data
            max_tables: Limit number of tables per schema
            recreate: If True, delete existing collection first
        """
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

    def search_tables(self, query: str, top_k: int = 3) -> List[dict]:
        """
        Search for relevant tables based on a natural language query.

        Args:
            query: User's natural language query
            top_k: Number of relevant tables to return

        Returns:
            List of dicts with table info and relevance scores
        """
        # Connect to vector store
        client = chromadb.PersistentClient(path=SCHEMA_VECTOR_DB_PATH)

        try:
            collection = client.get_collection(SCHEMA_COLLECTION_NAME)
        except:
            print(f"Schema collection '{SCHEMA_COLLECTION_NAME}' not found. Run build_schema_index() first.")
            return []

        # Perform similarity search
        query_embedding = self.embeddings.embed_query(query)
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            include=['documents', 'metadatas', 'distances']
        )

        # Format results
        relevant_tables = []
        if results['documents'] and len(results['documents'][0]) > 0:
            for i, (doc, metadata, distance) in enumerate(zip(
                results['documents'][0],
                results['metadatas'][0],
                results['distances'][0]
            )):
                relevant_tables.append({
                    'rank': i + 1,
                    'table_name': metadata['table_name'],
                    'full_name': metadata['full_name'],
                    'metadata': doc,
                    'relevance_score': 1 - distance,  # Convert distance to similarity
                    'connector_type': metadata['connector_type']
                })

        return relevant_tables


def build_schema_index_for_snowflake(
    database: Optional[str] = None,
    schema: Optional[str] = None,
    include_sample: bool = False,
    max_tables: Optional[int] = None
):
    """
    Convenience function to build schema index for Snowflake.

    Usage:
        from src.retrieval.schema_indexer import build_schema_index_for_snowflake
        build_schema_index_for_snowflake(schema='PUBLIC', max_tables=10)
    """
    indexer = SchemaIndexer('snowflake')
    indexer.build_schema_index(
        database=database,
        schema=schema,
        include_sample=include_sample,
        max_tables=max_tables
    )


if __name__ == "__main__":
    # Build schema index when run directly
    print("Building schema index for Snowflake...")
    print("Using database and schema from settings.yaml (or current connection defaults)")
    build_schema_index_for_snowflake(
        # database and schema will be read from settings.yaml
        include_sample=False,
        max_tables=None  # None = all tables
    )
