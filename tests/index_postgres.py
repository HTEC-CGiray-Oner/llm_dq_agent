#!/usr/bin/env python3
"""Index PostgreSQL tables and append to existing index"""
from src.retrieval.schema_indexer import SchemaIndexer

print("Indexing PostgreSQL tables...")
print("This will APPEND to existing index (keeps Snowflake tables)")
print("=" * 70)

indexer = SchemaIndexer(connector_type='postgres')
indexer.build_schema_index(
    recreate=False  # APPEND - don't delete Snowflake tables
)

print("\n✓ Done! Now you have both Snowflake and PostgreSQL tables indexed.")
print("\nRun check_index.py to verify.")
