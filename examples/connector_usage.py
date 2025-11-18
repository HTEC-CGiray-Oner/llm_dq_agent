# Example: Using the Multi-Connector DQ Agent
"""
This script demonstrates how to use the LLM Data Quality Agent
with different data source connectors.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agent.smart_planner import run_smart_dq_check
from src.data_quality.checks import check_dataset_duplicates

def example_1_snowflake_with_agent():
    """Example 1: Using the agent with Snowflake (requires credentials)."""
    print("\n" + "="*70)
    print("EXAMPLE 1: Agent with Snowflake (Natural Language Query)")
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
        result = run_smart_dq_check(query)
        print(f"\nAgent Response:\n{result['output']}")
    except Exception as e:
        print(f"\nError: {str(e)}")

def example_2_postgres_with_agent():
    """Example 2: Using the agent with PostgreSQL (requires credentials)."""
    print("\n" + "="*70)
    print("EXAMPLE 2: Agent with PostgreSQL (Natural Language Query)")
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
        result = run_smart_dq_check(query)
        print(f"\nAgent Response:\n{result['output']}")
    except Exception as e:
        print(f"\nError: {str(e)}")

def main():
    """Run all examples."""
    print("\n" + "="*70)
    print("Multi-Connector Data Quality Agent - Usage Examples")
    print("="*70)

    # Examples that require database configuration
    example_1_snowflake_with_agent()
    example_2_postgres_with_agent()

    print("\n" + "="*70)
    print("Examples completed!")
    print("="*70)
    print("\n📚 For more information, see docs/CONNECTORS.md")

if __name__ == "__main__":
    main()
