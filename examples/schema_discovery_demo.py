# Complete Example: Automatic Table Discovery with Schema Indexing

"""
This example shows the complete workflow:
1. Discover all tables from Snowflake and/or PostgreSQL
2. Index their metadata into a vector database
3. Use natural language to find the right table
4. Run data quality checks automatically

Usage:
    poetry run python examples/schema_discovery_demo.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.retrieval.schema_indexer import SchemaIndexer, build_schema_index_for_snowflake
from src.agent.smart_planner import run_smart_dq_check


def step1_build_schema_index():
    """
    Step 1: Discover and index all tables from data sources.
    This only needs to be done once (or when schema changes).
    """
    print("\n" + "="*80)
    print("STEP 1: BUILDING SCHEMA INDEX")
    print("="*80)
    print("""
This will:
1. Connect to your data source(s)
2. Auto-discover ALL schemas from INFORMATION_SCHEMA
3. Query all tables in each schema
4. Extract metadata (columns, types, row counts, comments)
5. Embed metadata using AI model (384-dimensional vectors)
6. Store in ChromaDB vector database

Configuration in config/settings.yaml controls which schemas to index.
    """)

    print("\nWhich connector(s) do you want to index?")
    print("1. Snowflake only")
    print("2. PostgreSQL only")
    print("3. Both (Snowflake + PostgreSQL)")

    choice = input("\nEnter choice (1-3): ").strip()

    if choice == '1':
        print("\nIndexing Snowflake tables...")
        indexer = SchemaIndexer(connector_type='snowflake')
        indexer.build_schema_index(recreate=True)

    elif choice == '2':
        print("\nIndexing PostgreSQL tables...")
        indexer = SchemaIndexer(connector_type='postgres')
        indexer.build_schema_index(recreate=True)

    elif choice == '3':
        print("\nIndexing Snowflake tables...")
        snowflake_indexer = SchemaIndexer(connector_type='snowflake')
        snowflake_indexer.build_schema_index(recreate=True)

        print("\nIndexing PostgreSQL tables...")
        postgres_indexer = SchemaIndexer(connector_type='postgres')
        postgres_indexer.build_schema_index(recreate=False)  # Append

    else:
        print("Invalid choice, skipping indexing")
        return

    print("\n✓ Schema index built successfully!")
    print("  This only needs to be done once (or when schema changes)")


def step2_search_tables():
    """
    Step 2: Search for tables using natural language.
    """
    print("\n" + "="*80)
    print("STEP 2: SEARCHING FOR RELEVANT TABLES")
    print("="*80)

    indexer = SchemaIndexer()  # Connector-agnostic search

    # Example queries
    test_queries = [
        "customer information and contact details",
        "product data",
    ]

    for query in test_queries:
        print(f"\n📝 Query: '{query}'")
        print("-" * 80)

        results = indexer.search_tables(query, top_k=3)

        if results:
            for table in results:
                print(f"\n  {table['rank']}. {table['full_name']}")
                print(f"     Relevance: {table['relevance_score']:.2%}")
                # Show first few lines of metadata
                metadata_preview = table['metadata'].split('\n')[:5]
                for line in metadata_preview:
                    print(f"     {line}")
        else:
            print("  No tables found")

    input("\nPress Enter to continue...")


def step3_automatic_dq_check():
    """
    Step 3: Run data quality check with automatic table discovery.
    """
    print("\n" + "="*80)
    print("STEP 3: AUTOMATIC DATA QUALITY CHECK")
    print("="*80)
    print("""
Now the agent will:
1. Take your natural language query
2. Automatically find relevant tables via RAG
3. Select the best matching table
4. Run the data quality check
5. Return results
    """)

    input("Press Enter to continue...")

    # Example DQ queries
    print("\nExample queries:")
    print("  - 'how many duplicate rows are in the snowflake products table?'")
    print("  - 'how many duplicate rows are in the postgres customers table?'")

    dq_queries = [
        "how many duplicate rows are in the snowflake products table?",
        "how many duplicate rows are in the postgres customers table?",
    ]

    for query in dq_queries:
        print(f"\n{'='*80}")
        result = run_smart_dq_check(query, top_k_tables=3)
        print(f"\n📊 Final Result:")
        print(f"{result['output']}")
        print(f"\n{'='*80}\n")

        input("Press Enter for next query...")


def demo_workflow():
    """Run the complete demonstration."""
    print("\n" + "="*80)
    print("AUTOMATIC TABLE DISCOVERY & DATA QUALITY CHECK")
    print("Complete Workflow Demonstration")
    print("="*80)

    print("""
This demo shows how the system:
- Automatically discovers tables from multiple data sources (Snowflake, PostgreSQL)
- Indexes table metadata (columns, types, descriptions)
- Uses AI embeddings to find relevant tables from natural language
- Distinguishes between tables from different connectors
- Runs data quality checks on the right table without manual specification

Prerequisites:
✓ Database credentials configured in .env and config/settings.yaml
✓ Connections working (test with: poetry install -E snowflake -E postgres)
    """)

    choice = input("\nChoose an option:\n"
                  "1. Build schema index (run first time)\n"
                  "2. Search for tables\n"
                  "3. Run automatic DQ check\n"
                  "4. Full demo (all steps)\n"
                  "Enter choice (1-4): ")

    if choice == '1':
        step1_build_schema_index()
    elif choice == '2':
        step2_search_tables()
    elif choice == '3':
        step3_automatic_dq_check()
    elif choice == '4':
        step1_build_schema_index()
        step2_search_tables()
        step3_automatic_dq_check()
    else:
        print("Invalid choice")


if __name__ == "__main__":
    demo_workflow()
