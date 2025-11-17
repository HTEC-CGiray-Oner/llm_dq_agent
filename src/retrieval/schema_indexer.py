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

    def build_schema_index(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_sample: bool = False,
        max_tables: Optional[int] = None,
        recreate: bool = True
    ):
        """
        Discover all tables and index their metadata.

        Args:
            database: Database to scan
            schema: Schema to scan
            include_sample: Include sample data
            max_tables: Limit number of tables
            recreate: If True, delete existing collection first
        """
        print(f"\n{'='*70}")
        print("BUILDING SCHEMA INDEX")
        print(f"{'='*70}")

        # Discover all table metadata
        metadata_docs = self.discovery.discover_all_table_metadata(
            database=database,
            schema=schema,
            include_sample=include_sample,
            max_tables=max_tables
        )

        if not metadata_docs:
            print("No tables found to index")
            return

        # Prepare data for indexing
        texts = [doc['metadata'] for doc in metadata_docs]
        metadatas = [
            {
                'table_name': doc['table_name'],
                'full_name': doc['full_name'],
                'connector_type': self.connector_type
            }
            for doc in metadata_docs
        ]

        # Create ChromaDB client
        client = chromadb.PersistentClient(path=SCHEMA_VECTOR_DB_PATH)

        # Delete existing collection if recreate=True
        if recreate:
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

        print(f"\n{'='*70}")
        print(f"✓ SCHEMA INDEX BUILT SUCCESSFULLY")
        print(f"  - Collection: {SCHEMA_COLLECTION_NAME}")
        print(f"  - Tables indexed: {len(texts)}")
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
    build_schema_index_for_snowflake(
        schema='PUBLIC',
        include_sample=False,
        max_tables=20  # Limit for testing
    )
