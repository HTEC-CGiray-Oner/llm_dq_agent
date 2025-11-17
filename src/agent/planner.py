# src/agent/planner.py
import os
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file
from langchain_community.vectorstores import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_openai import ChatOpenAI # Requires API key config
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.tools import StructuredTool
from src.data_quality.checks import check_dataset_duplicates # Import function directly
from src.retrieval.index_builder import VECTOR_DB_PATH # Import path

# Convert the function to a LangChain tool
DQ_TOOLS = [
    StructuredTool.from_function(
        func=check_dataset_duplicates,
        name="check_dataset_duplicates",
        description=check_dataset_duplicates.__doc__
    )
]

# --- 1. RAG Component Setup ---
def get_retriever():
    """Initializes the RAG retrieval component."""
    embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

    # Reconnect to the persistent Chroma DB
    vectorstore = Chroma(
        persist_directory=VECTOR_DB_PATH,
        embedding_function=embeddings,
        collection_name="dq_methods"
    )
    # The retriever finds the top 3 relevant method descriptions
    return vectorstore.as_retriever(search_kwargs={"k": 3})

# --- 2. Agent Creation ---
def create_dq_agent():
    """Creates the LLM agent and links the RAG-retrieved context."""

    # A. Define the Prompt
    prompt = ChatPromptTemplate.from_messages([
        ("system",
         """You are an expert Data Quality Agent. Your task is to identify and execute the most appropriate data quality check function based on the user's request and the PROVIDED TOOL CONTEXT.

         You must infer these parameters from the user's input:
         1. 'dataset_id' - the name of the table/file/dataset (REQUIRED)
         2. 'connector_type' - the data source type (OPTIONAL: 'snowflake', 'postgres', or 'csv')

         Examples of connector type extraction:
         - "check duplicates in Snowflake table CUSTOMERS" → connector_type='snowflake', dataset_id='CUSTOMERS'
         - "analyze postgres table users" → connector_type='postgres', dataset_id='users'
         - "check CSV file sales.csv" → connector_type='csv', dataset_id='sales.csv'
         - "check table ORDERS" → connector_type=None (will use default), dataset_id='ORDERS'

         The final output must be a function call with appropriate parameters, or a statement asking for clarification if the dataset_id is missing."""
        ),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])

    # B. Initialize LLM (using a tool-calling enabled model with custom base_url)
    llm = ChatOpenAI(
        temperature=0,
        model="l2-gpt-4o",
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_api_base=os.getenv("OPENAI_BASE_URL")
    )

    # C. Bind Tools and Create Executor
    # The agent is provided with the DQ_TOOLS list, which only contains check_dataset_duplicates
    agent = create_tool_calling_agent(llm, DQ_TOOLS, prompt)
    executor = AgentExecutor(agent=agent, tools=DQ_TOOLS, verbose=True)

    return executor

# --- 3. Execution Loop ---
def run_dq_check(query: str):
    """Retrieves context and executes the agent."""

    # RAG Retrieval Step: Find relevant tools based on the query
    retriever = get_retriever()
    retrieved_docs = retriever.invoke(query)

    # Inject the RAG context into the LLM input
    rag_context = (
        f"AVAILABLE TOOLS CONTEXT (Retrieved via RAG):\n"
        f"{[doc.page_content for doc in retrieved_docs]}"
    )

    # Combine user query and RAG context
    full_input = f"User Request: {query}\n\n{rag_context}"

    dq_agent = create_dq_agent()
    result = dq_agent.invoke({"input": full_input, "chat_history": []})

    # The result['output'] will contain the natural language summary of the function's return value
    return result

if __name__ == "__main__":
    # Example: User asks for a check, providing the dataset name
    query_1 = "I need to check for any duplicated rows in the 'Q4_invoice_data' table."
    result_1 = run_dq_check(query_1)

    print("\n--- Final Agent Output ---")
    print(result_1['output'])
