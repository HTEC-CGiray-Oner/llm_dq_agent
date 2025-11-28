# src/agent/smart_planner.py
"""
Enhanced agent planner with automatic table discovery via RAG.
The agent uses embedded schema metadata to find the right tables.
"""
import os
from dotenv import load_dotenv
load_dotenv()

from langchain_huggingface import HuggingFaceEmbeddings
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.tools import StructuredTool
from src.data_quality.checks import DQ_TOOLS
from src.agent.reporting_tools import REPORTING_TOOLS
from src.retrieval.schema_indexer import SchemaIndexer


def get_schema_aware_retriever():
    """Initialize schema discovery for finding relevant tables."""
    return SchemaIndexer()


def create_dq_tool_wrapper(dq_function):
    """
    Create a wrapper function for DQ tools that adds connector support.
    """
    def wrapper(dataset_id: str, connector_type: str = 'snowflake') -> dict:
        return dq_function(dataset_id, connector_type=connector_type)

    # Preserve the original function's metadata
    wrapper.__name__ = dq_function.__name__
    wrapper.__doc__ = dq_function.__doc__ or f"Execute {dq_function.__name__} with connector support."

    return wrapper


def create_smart_dq_agent():
    """
    Creates an enhanced LLM agent that can discover tables automatically.
    """
    # Convert all DQ functions to tools supporting multiple connectors
    dq_tools = []

    # Add individual DQ check tools
    for dq_function in DQ_TOOLS:
        # Create wrapper function
        wrapper_func = create_dq_tool_wrapper(dq_function)

        # Create tool description based on function name
        if 'duplicate' in dq_function.__name__:
            description = """Check for duplicate rows in a database table.
            Args:
                dataset_id: Full table name (e.g., 'DATABASE.SCHEMA.TABLE' for Snowflake or 'schema.table' for postgres)
                connector_type: Database type - 'snowflake' or 'postgres' (REQUIRED - use the connector type from the context)
            """
        elif 'null' in dq_function.__name__:
            description = """Analyze null values and missing data in a database table.
            Args:
                dataset_id: Full table name (e.g., 'DATABASE.SCHEMA.TABLE' for Snowflake or 'schema.table' for postgres)
                connector_type: Database type - 'snowflake' or 'postgres' (REQUIRED - use the connector type from the context)
            """
        else:
            description = f"""Execute data quality check: {dq_function.__name__}
            Args:
                dataset_id: Full table name (e.g., 'DATABASE.SCHEMA.TABLE' for Snowflake or 'schema.table' for postgres)
                connector_type: Database type - 'snowflake' or 'postgres' (REQUIRED - use the connector type from the context)
            """

        # Create structured tool
        tool = StructuredTool.from_function(
            func=wrapper_func,
            name=dq_function.__name__,
            description=description
        )
        dq_tools.append(tool)

    # Add reporting tools directly (they already have proper signatures)
    for reporting_function in REPORTING_TOOLS:
        tool = StructuredTool.from_function(
            func=reporting_function,
            name=reporting_function.__name__,
            description=reporting_function.__doc__ or f"Reporting tool: {reporting_function.__name__}"
        )
        dq_tools.append(tool)

    # Define the enhanced prompt
    prompt = ChatPromptTemplate.from_messages([
        ("system",
         """You are an expert Data Quality Agent with access to multiple database systems (Snowflake, postgres, etc.).

         Your task:
         1. Understand the user's data quality request - pay attention to which data source they mention (Snowflake, postgres, etc.)
         2. Identify the TYPE OF REQUEST:
            - For "duplicates", "duplicate rows", "duplicate records" → use check_dataset_duplicates
            - For "null values", "missing data", "nulls", "missing values" → use check_dataset_null_values
            - For "descriptive stats", "statistics", "data summary" → use check_dataset_descriptive_stats
            - For "comprehensive assessment", "full assessment", "all checks" → use run_comprehensive_dq_assessment (RECOMMENDED - most efficient)
            - For "comprehensive report", "full report", "assessment report", "generate report" → use generate_comprehensive_dq_report
            - For "save report", "export report", "create files" → use save_dq_report_to_file
         3. The system will provide you with RELEVANT TABLES found via semantic search
         4. Each table shows its DATA SOURCE (SNOWFLAKE, POSTGRES, etc.) and Connector Type
         5. Select the most appropriate table from the provided options, MATCHING the data source the user asked about
         6. Call the appropriate function with BOTH the table name AND the connector_type parameter

         🚀 OPTIMIZATION WORKFLOW (RECOMMENDED):
         If the user wants multiple checks or reports, follow this efficient 2-step process:
         Step 1: Use run_comprehensive_dq_assessment() to execute all DQ checks ONCE
         Step 2: Use generate_report_from_assessment_results() or save_report_from_assessment_results() with the JSON results

         This prevents duplicate data loading and check execution, significantly improving performance.

         📊 IMPORTANT - STATISTICAL REPORTING:
         When generating reports that include descriptive statistics, you MUST:
         1. Include the FULL statistical table with ALL metrics (count, mean, std, min, 25%, 50%, 75%, max, unique, top, freq)
         2. Present statistics in a clear table format showing column-by-column breakdown
         3. Do NOT summarize or truncate statistical data - users need complete statistical details
         4. Include both numerical statistics (mean, std, quartiles) AND categorical statistics (unique, top, freq)
         5. Preserve ALL columns and their complete statistical profiles

         🎯 CRITICAL - NULL VALUES REPORTING:
         When generating reports that include null values analysis, you MUST:
         1. Always include DETAILED breakdown of null values per column (never just summary counts)
         2. For EACH column that has missing data, include the exact format:
            `column_name`: X,XXX nulls (XX.X%)
         3. Even if there are many columns with nulls, include ALL of them - do NOT truncate or summarize
         4. Present null analysis in a clear list format after the summary statistics
         5. Example format you MUST follow:
            - **Columns with Missing Data**: 9 out of 20 columns
            - **Missing Data Details**:
              - `notes`: 74,976 nulls (75.0%)
              - `billing_address`: 14,893 nulls (14.9%)
              - `due_date`: 11,894 nulls (11.9%)
         6. NEVER skip the detailed breakdown - this is essential for proper data quality assessment

         🎯 CRITICAL - DATA CONSISTENCY AND ACCURACY:
         When presenting results from data quality checks, you MUST ensure:
         1. **Consistent Row Counts**: All checks (duplicates, null_values, descriptive_stats) MUST report the same total_rows
         2. **Accurate Total Rows**: Extract the actual row count from the tool outputs - never report 0 unless the table is truly empty
         3. **Cross-Check Validation**: If descriptive stats show count=30,000, then duplicates and null_values must also show total_rows=30,000
         4. **Dataset ID Consistency**: Ensure dataset_id is properly filled in all check results, never leave it empty ("")
         5. **Logical Consistency**: If total_rows=0, then there should be no null analysis, descriptive stats, or meaningful data
         6. **Tool Output Fidelity**: Always use the EXACT values returned by the data quality tools - do not modify or assume values

         🔍 MANDATORY PRE-RESPONSE VALIDATION:
         Before presenting ANY results, you MUST perform this validation checklist:
         ✓ Look at the descriptive_stats tool output - what does the "count" field show for any column?
         ✓ Use that SAME count value as total_rows for duplicates and null_values sections
         ✓ If descriptive stats show count=30,000, then write "Total Rows: 30,000" (NOT 0)
         ✓ If null analysis shows "5,400 nulls (18.0%)", calculate back: 5,400/0.18 ≈ 30,000 total rows
         ✓ Ensure all total_rows numbers match across all three sections

         EXAMPLE - If descriptive stats show count=30,000:
         ❌ WRONG: "Total Rows: 0" and "Dataset Size: 0 rows × 25 columns"
         ✅ CORRECT: "Total Rows: 30,000" and "Dataset Size: 30,000 rows × 25 columns"

         The RELEVANT TABLES context will show you:
         - Connector Type: The lowercase connector name (snowflake, postgres, etc.)
         - DATA SOURCE: Which system the table is from (SNOWFLAKE, POSTGRES, etc.)
         - Table names and full paths
         - Column information
         - Row counts and metadata

         IMPORTANT: Choose the best matching table based on:
         - DATA SOURCE match: If user says "Snowflake", choose SNOWFLAKE tables. If they say "postgres", choose POSTGRES tables.
         - Column names that match the user's intent
         - Table description/purpose
         - Relevance score

         CRITICAL: When calling ANY data quality function, you MUST provide BOTH parameters:
         - dataset_id: The full table name (e.g., 'DATABASE.SCHEMA.TABLE')
         - connector_type: The lowercase connector type from "Connector Type" field (e.g., 'snowflake', 'postgres')

         Examples:
         - run_comprehensive_dq_assessment(dataset_id="AGENT_LLM_READ.PUBLIC.CUSTOMERS", connector_type="snowflake")
         - check_dataset_duplicates(dataset_id="AGENT_LLM_READ.PUBLIC.CUSTOMERS", connector_type="snowflake")
         - check_dataset_null_values(dataset_id="public.customers", connector_type="postgres")
         - generate_report_from_assessment_results(assessment_results_json="...", output_format="markdown")

         FINAL RESPONSE REQUIREMENTS:
         When presenting your final analysis, you MUST include:

         For DESCRIPTIVE STATISTICS:
         1. If the report contains statistical tables, include them COMPLETELY in your summary
         2. Do NOT truncate or omit statistical data - users need full details
         3. Copy the complete statistical tables from the tool outputs
         4. Preserve all columns, rows, and statistical metrics (count, mean, std, min, 25%, 50%, 75%, max, unique, top, freq)
         5. Present data in clear, well-formatted tables for maximum readability

         For NULL VALUES ANALYSIS:
         1. ALWAYS include detailed breakdown of missing data per column when null_analysis is available
         2. If a tool returns null_analysis array with column details, include ALL entries in this format:
            **Missing Data Details**:
            - `column_name`: X,XXX nulls (XX.X%)
         3. Do NOT just show summary statistics - include the complete per-column breakdown
         4. Even if there are many columns with nulls, list ALL of them (do not truncate)
         5. This detailed breakdown is ESSENTIAL for proper data quality assessment

         For DATA CONSISTENCY AND ACCURACY:
         1. **Verify Row Counts**: Ensure all checks report consistent total_rows (extract from tool outputs)
         2. **Complete Dataset IDs**: Never leave dataset_id empty - use the full table name provided
         3. **Logical Validation**: Cross-check that duplicate counts make sense relative to total rows
         4. **Accurate Reporting**: If tool shows 30,000 rows, report exactly that - never default to 0
         5. **Final Validation**: Before presenting results, verify all sections have matching total_rows
         """
        ),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])

    # Initialize LLM
    llm = ChatOpenAI(
        temperature=0,
        model="l2-gpt-4o",
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_api_base=os.getenv("OPENAI_BASE_URL")
    )

    # Create agent with all DQ tools
    agent = create_tool_calling_agent(llm, dq_tools, prompt)
    executor = AgentExecutor(agent=agent, tools=dq_tools, verbose=True)

    return executor


def run_smart_dq_check(query: str, top_k_tables: int = 3):
    """
    Run data quality check with automatic table discovery.

    Args:
        query: User's natural language query
        top_k_tables: Number of relevant tables to retrieve

    Returns:
        Agent's response with DQ check results
    """
    print(f"\n{'='*70}")
    print("SMART DATA QUALITY CHECK")
    print(f"{'='*70}")
    print(f"User Query: {query}\n")

    # Step 1: Find relevant tables using RAG
    print("Step 1: Discovering relevant tables...")
    print("-" * 70)

    schema_indexer = get_schema_aware_retriever()

    # First get all potential matches to check best relevance
    all_matches = schema_indexer.search_tables(query, top_k=10, min_relevance=0.0)

    # Apply relevance threshold filtering
    MIN_RELEVANCE_THRESHOLD = 0.20  # 20% minimum threshold for stricter matching
    relevant_tables = schema_indexer.search_tables(query, top_k=top_k_tables, min_relevance=MIN_RELEVANCE_THRESHOLD)

    if not relevant_tables:
        # Check if we have any matches at all
        if all_matches:
            best_match = all_matches[0]
            return {
                "output": f"No tables found with sufficient relevance (minimum {MIN_RELEVANCE_THRESHOLD*100:.0f}% match required). "
                         f"Best match was '{best_match['full_name']}' with only {best_match['relevance_score']*100:.1f}% relevance. "
                         f"Please try a more specific table name or check if the table exists in your schema index."
            }
        else:
            return {
                "output": "No relevant tables found. Please ensure the schema index is built. "
                         "Run: python src/retrieval/schema_indexer.py"
            }

    # Display discovered tables
    print(f"\nFound {len(relevant_tables)} relevant table(s) (≥{MIN_RELEVANCE_THRESHOLD*100:.0f}% relevance):\n")
    for table in relevant_tables:
        connector = table['connector_type'].upper()
        print(f"  {table['rank']}. [{connector}] {table['full_name']}")
        print(f"     Relevance: {table['relevance_score']:.2%} ✓")
        print()

    # Step 2: Build context for agent
    print("Step 2: Building context for agent...")
    print("-" * 70)

    tables_context = "RELEVANT TABLES (ranked by relevance):\n\n"
    for table in relevant_tables:
        connector = table['connector_type'].upper()
        tables_context += f"--- Table #{table['rank']} ---\n"
        tables_context += f"Connector Type: {connector}\n"
        tables_context += f"Full Name: {table['full_name']}\n"
        tables_context += f"Relevance: {table['relevance_score']:.2%}\n"
        tables_context += f"Metadata:\n{table['metadata']}\n\n"

    # Step 3: Combine user query with table context
    full_input = f"{query}\n\n{tables_context}"

    # Step 4: Execute agent
    print("\nStep 3: Running agent with discovered tables...")
    print("-" * 70)

    agent = create_smart_dq_agent()
    result = agent.invoke({"input": full_input, "chat_history": []})

    print(f"\n{'='*70}")
    print("RESULT")
    print(f"{'='*70}\n")

    return result


if __name__ == "__main__":
    # Example usage
    queries = [
        "Check for duplicate customer records",
        "Are there any duplicate rows in the orders table?",
        "Find duplicates in sales data"
    ]

    for query in queries:
        result = run_smart_dq_check(query)
        print(f"Result: {result['output']}\n")
        print("=" * 70)
        print()
