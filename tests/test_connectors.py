# Test script for connector functionality
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.connectors.connector_factory import ConnectorFactory

def test_snowflake_connector():
    """Test Snowflake connector (requires credentials in .env)."""
    print("\n=== Testing Snowflake Connector ===")
    try:
        connector = ConnectorFactory.create_connector('snowflake')

        # Test connection
        if connector.test_connection():
            print("\n✓ Snowflake connector test passed!")
            return True
        else:
            print("\n✗ Snowflake connection failed")
            return False
    except Exception as e:
        print(f"\n✗ Snowflake connector test failed: {str(e)}")
        print("Note: Configure Snowflake credentials in .env to test this connector")
        return False

def test_postgres_connector():
    """Test PostgreSQL connector (requires credentials in .env)."""
    print("\n=== Testing PostgreSQL Connector ===")
    try:
        connector = ConnectorFactory.create_connector('postgres')

        # Test connection
        if connector.test_connection():
            print("\n✓ PostgreSQL connector test passed!")
            return True
        else:
            print("\n✗ PostgreSQL connection failed")
            return False
    except Exception as e:
        print(f"\n✗ PostgreSQL connector test failed: {str(e)}")
        print("Note: Configure PostgreSQL credentials in .env to test this connector")
        return False

def test_dq_check_with_connector():
    """Test DQ check with connector integration."""
    print("\n=== Testing DQ Check with Snowflake Connector ===")
    try:
        from src.data_quality.checks import check_dataset_duplicates

        # Test with Snowflake (requires credentials)
        result = check_dataset_duplicates('AGENT_LLM_READ.PUBLIC.CUSTOMERS', connector_type='snowflake')
        print(f"\nResult: {result}")
        print(f"✓ Found {result['duplicate_qty']} duplicate rows")
        return True
    except Exception as e:
        print(f"\n✗ DQ check test failed: {str(e)}")
        print("Note: Configure Snowflake credentials in .env to test this")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("Data Source Connector Test Suite")
    print("=" * 60)

    results = []

    # Test Snowflake (requires credentials)
    results.append(("Snowflake Connector", test_snowflake_connector()))

    # Test PostgreSQL (requires credentials)
    results.append(("PostgreSQL Connector", test_postgres_connector()))

    # Test DQ check integration
    results.append(("DQ Check Integration", test_dq_check_with_connector()))

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name}: {status}")

    print("\n" + "=" * 60)
