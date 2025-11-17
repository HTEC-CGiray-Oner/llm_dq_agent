# Example: Using the Multi-Connector DQ Agent
"""
This script demonstrates how to use the LLM Data Quality Agent
with different data source connectors.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agent.planner import run_dq_check
from src.data_quality.checks import check_dataset_duplicates

def example_1_csv_with_agent():
    """Example 1: Using the agent with CSV files (natural language)."""
    print("\n" + "="*70)
    print("EXAMPLE 1: Agent with CSV (Natural Language Query)")
    print("="*70)

    query = "Check for duplicates in the CSV file sample_data.csv"
    print(f"\nQuery: {query}")
    print("\nAgent is processing...")

    try:
        result = run_dq_check(query)
        print(f"\nAgent Response:\n{result['output']}")
    except Exception as e:
        print(f"\nError: {str(e)}")

def example_2_csv_direct():
    """Example 2: Direct function call with CSV connector."""
    print("\n" + "="*70)
    print("EXAMPLE 2: Direct Function Call with CSV")
    print("="*70)

    print("\nCalling: check_dataset_duplicates('sample_data', connector_type='csv')")

    try:
        result = check_dataset_duplicates('sample_data', connector_type='csv')
        print(f"\nResult: {result}")
        print(f"\n✓ Dataset: {result['dataset_id']}")
        print(f"✓ Duplicates Found: {result['duplicate_qty']}")
        print(f"✓ Status: {result['status']}")
    except Exception as e:
        print(f"\nError: {str(e)}")

def example_3_snowflake_with_agent():
    """Example 3: Using the agent with Snowflake (requires credentials)."""
    print("\n" + "="*70)
    print("EXAMPLE 3: Agent with Snowflake (Natural Language Query)")
    print("="*70)

    # Check if Snowflake credentials are configured
    if not os.getenv('SNOWFLAKE_ACCOUNT'):
        print("\n⚠️  Snowflake credentials not configured in .env")
        print("To test Snowflake, add the following to your .env file:")
        print("  SNOWFLAKE_ACCOUNT=your_account.region")
        print("  SNOWFLAKE_USER=your_username")
        print("  SNOWFLAKE_PASSWORD=your_password")
        print("  SNOWFLAKE_WAREHOUSE=your_warehouse")
        print("  SNOWFLAKE_DATABASE=your_database")
        print("  SNOWFLAKE_SCHEMA=your_schema")
        return

    query = "Check for duplicate rows in Snowflake table CUSTOMERS"
    print(f"\nQuery: {query}")
    print("\nAgent is processing...")

    try:
        result = run_dq_check(query)
        print(f"\nAgent Response:\n{result['output']}")
    except Exception as e:
        print(f"\nError: {str(e)}")

def example_4_default_connector():
    """Example 4: Using default connector from settings."""
    print("\n" + "="*70)
    print("EXAMPLE 4: Using Default Connector (from settings.yaml)")
    print("="*70)

    print("\nCalling: check_dataset_duplicates('sample_data')")
    print("(connector_type not specified, will use default from settings.yaml)")

    try:
        result = check_dataset_duplicates('sample_data')
        print(f"\nResult: {result}")
        print(f"\n✓ Used default connector successfully!")
    except Exception as e:
        print(f"\nError: {str(e)}")

def example_5_postgres_with_agent():
    """Example 5: Using the agent with PostgreSQL (requires credentials)."""
    print("\n" + "="*70)
    print("EXAMPLE 5: Agent with PostgreSQL (Natural Language Query)")
    print("="*70)

    # Check if PostgreSQL credentials are configured
    if not os.getenv('POSTGRES_HOST'):
        print("\n⚠️  PostgreSQL credentials not configured in .env")
        print("To test PostgreSQL, add the following to your .env file:")
        print("  POSTGRES_HOST=localhost")
        print("  POSTGRES_PORT=5432")
        print("  POSTGRES_DATABASE=your_database")
        print("  POSTGRES_USER=your_username")
        print("  POSTGRES_PASSWORD=your_password")
        return

    query = "Analyze the postgres table users for duplicate rows"
    print(f"\nQuery: {query}")
    print("\nAgent is processing...")

    try:
        result = run_dq_check(query)
        print(f"\nAgent Response:\n{result['output']}")
    except Exception as e:
        print(f"\nError: {str(e)}")

def main():
    """Run all examples."""
    print("\n" + "="*70)
    print("Multi-Connector Data Quality Agent - Usage Examples")
    print("="*70)

    # Examples that should work out of the box (CSV)
    example_2_csv_direct()
    example_4_default_connector()
    example_1_csv_with_agent()

    # Examples that require additional configuration
    example_3_snowflake_with_agent()
    example_5_postgres_with_agent()

    print("\n" + "="*70)
    print("Examples completed!")
    print("="*70)
    print("\n📚 For more information, see docs/CONNECTORS.md")

if __name__ == "__main__":
    main()
