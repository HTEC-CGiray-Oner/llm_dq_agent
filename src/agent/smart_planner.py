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
from src.data_quality.checks import check_dataset_duplicates
from src.retrieval.schema_indexer import SchemaIndexer


def get_schema_aware_retriever():
    """Initialize schema discovery for finding relevant tables."""
    return SchemaIndexer(connector_type='snowflake')


def check_duplicates_snowflake(dataset_id: str) -> dict:
    """
    Check for duplicate rows in a Snowflake table.

    Args:
        dataset_id: Full table name (e.g., 'DATABASE.SCHEMA.TABLE')

    Returns:
        Dictionary with duplicate count and status
    """
    return check_dataset_duplicates(dataset_id, connector_type='snowflake')


def create_smart_dq_agent():
    """
    Creates an enhanced LLM agent that can discover tables automatically.
    """
    # Convert DQ function to tool with Snowflake connector
    dq_tool = StructuredTool.from_function(
        func=check_duplicates_snowflake,
        name="check_dataset_duplicates",
        description="Check for duplicate rows in a Snowflake table. Provide the full table name (DATABASE.SCHEMA.TABLE)."
    )

    # Define the enhanced prompt
    prompt = ChatPromptTemplate.from_messages([
        ("system",
         """You are an expert Data Quality Agent with access to a Snowflake database schema discovery system.

         Your task:
         1. Understand the user's data quality request
         2. The system will provide you with RELEVANT TABLES from Snowflake found via semantic search
         3. Select the most appropriate table from the provided options
         4. Call the data quality check function with the full table name

         The RELEVANT TABLES context will show you:
         - Table names and full paths
         - Column information
         - Row counts and metadata

         Choose the best matching table based on:
         - Column names that match the user's intent
         - Table description/purpose
         - Relevance score

         Always use the full table name (DATABASE.SCHEMA.TABLE) when calling the check_dataset_duplicates function.
         The system will automatically use the Snowflake connector.
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

    # Create agent
    agent = create_tool_calling_agent(llm, [dq_tool], prompt)
    executor = AgentExecutor(agent=agent, tools=[dq_tool], verbose=True)

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
        print(f"  {table['rank']}. {table['full_name']}")
        print(f"     Relevance: {table['relevance_score']:.2%}")
        print()

    # Step 2: Build context for agent
    print("Step 2: Building context for agent...")
    print("-" * 70)

    tables_context = "RELEVANT TABLES (ranked by relevance):\n\n"
    for table in relevant_tables:
        tables_context += f"--- Table #{table['rank']} ---\n"
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
