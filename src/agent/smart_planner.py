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
    return SchemaIndexer()  # No specific connector - search all


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
                dataset_id: Full table name (e.g., 'DATABASE.SCHEMA.TABLE' for Snowflake or 'schema.table' for PostgreSQL)
                connector_type: Database type - 'snowflake' or 'postgres' (REQUIRED - use the connector type from the context)
            """
        elif 'null' in dq_function.__name__:
            description = """Analyze null values and missing data in a database table.
            Args:
                dataset_id: Full table name (e.g., 'DATABASE.SCHEMA.TABLE' for Snowflake or 'schema.table' for PostgreSQL)
                connector_type: Database type - 'snowflake' or 'postgres' (REQUIRED - use the connector type from the context)
            """
        else:
            description = f"""Execute data quality check: {dq_function.__name__}
            Args:
                dataset_id: Full table name (e.g., 'DATABASE.SCHEMA.TABLE' for Snowflake or 'schema.table' for PostgreSQL)
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
         """You are an expert Data Quality Agent with access to multiple database systems (Snowflake, PostgreSQL, etc.).

         Your task:
         1. Understand the user's data quality request - pay attention to which data source they mention (Snowflake, PostgreSQL, etc.)
         2. Identify the TYPE OF REQUEST:
            - For "duplicates", "duplicate rows", "duplicate records" → use check_dataset_duplicates
            - For "null values", "missing data", "nulls", "missing values" → use check_dataset_null_values
            - For "descriptive stats", "statistics", "data summary" → use check_dataset_descriptive_stats
            - For "comprehensive report", "full report", "assessment report", "generate report" → use generate_comprehensive_dq_report
            - For "save report", "export report", "create files" → use save_dq_report_to_file
         3. The system will provide you with RELEVANT TABLES found via semantic search
         4. Each table shows its DATA SOURCE (SNOWFLAKE, POSTGRES, etc.) and Connector Type
         5. Select the most appropriate table from the provided options, MATCHING the data source the user asked about
         6. Call the appropriate function with BOTH the table name AND the connector_type parameter

         The RELEVANT TABLES context will show you:
         - Connector Type: The lowercase connector name (snowflake, postgres, etc.)
         - DATA SOURCE: Which system the table is from (SNOWFLAKE, POSTGRES, etc.)
         - Table names and full paths
         - Column information
         - Row counts and metadata

         IMPORTANT: Choose the best matching table based on:
         - DATA SOURCE match: If user says "Snowflake", choose SNOWFLAKE tables. If they say "PostgreSQL" or "Postgres", choose POSTGRES tables.
         - Column names that match the user's intent
         - Table description/purpose
         - Relevance score

         CRITICAL: When calling ANY data quality function, you MUST provide BOTH parameters:
         - dataset_id: The full table name (e.g., 'DATABASE.SCHEMA.TABLE')
         - connector_type: The lowercase connector type from "Connector Type" field (e.g., 'snowflake', 'postgres')

         Examples:
         - check_dataset_duplicates(dataset_id="AGENT_LLM_READ.PUBLIC.CUSTOMERS", connector_type="snowflake")
         - check_dataset_null_values(dataset_id="public.customers", connector_type="postgres")
         - generate_comprehensive_dq_report(dataset_id="AGENT_LLM_READ.PUBLIC.CUSTOMERS", connector_type="snowflake", output_format="summary")
         - save_dq_report_to_file(dataset_id="public.customers", connector_type="postgres", formats="markdown,html")
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
    relevant_tables = schema_indexer.search_tables(query, top_k=top_k_tables)

    if not relevant_tables:
        return {
            "output": "No relevant tables found. Please ensure the schema index is built. "
                     "Run: python src/retrieval/schema_indexer.py"
        }

    # Display discovered tables
    print(f"\nFound {len(relevant_tables)} relevant table(s):\n")
    for table in relevant_tables:
        connector = table['connector_type'].upper()
        print(f"  {table['rank']}. [{connector}] {table['full_name']}")
        print(f"     Relevance: {table['relevance_score']:.2%}")
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
