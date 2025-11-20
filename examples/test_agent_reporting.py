# test_agent_reporting.py
"""
Test script to demonstrate the enhanced agent with reporting capabilities.
"""
import os
import sys
sys.path.append('.')

from src.agent.smart_planner import run_smart_dq_check

def test_reporting_agent():
    """Test the agent with various reporting queries."""
    
    print("🤖 Testing Enhanced Data Quality Agent with Reporting")
    print("=" * 80)
    
    # Test queries for different types of requests
    test_queries = [
        # Individual checks
        "Check for duplicates in STAGE_SALES CUSTOMERS table",
        "Find null values in postgres customers data",
        
        # Comprehensive reporting
        "Generate a comprehensive data quality report for STAGE_SALES CUSTOMERS",
        "Create a full DQ assessment report for postgres customers table",
        
        # Save to files
        "Generate and save data quality report for STAGE_SALES CUSTOMERS to files",
        "Export comprehensive DQ report for postgres customers in markdown and HTML format"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n🔍 TEST {i}: {query}")
        print("-" * 80)
        
        try:
            result = run_smart_dq_check(query, top_k_tables=3)
            print(f"\n✅ Result: {result['output']}")
            
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
        
        print("\n" + "=" * 80)

if __name__ == "__main__":
    test_reporting_agent()