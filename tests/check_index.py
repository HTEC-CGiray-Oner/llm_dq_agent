#!/usr/bin/env python3
"""Check what's currently indexed"""
from src.retrieval.schema_indexer import SchemaIndexer

indexer = SchemaIndexer()
results = indexer.search_tables('*', top_k=10)

if results:
    print('Current indexed tables:')
    print('-' * 50)
    for r in results:
        print(f'{r["connector_type"].upper()}: {r["full_name"]}')
else:
    print('No tables indexed')
