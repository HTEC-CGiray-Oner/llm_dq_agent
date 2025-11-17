# src/retrieval/index_builder.py
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import SentenceTransformerEmbeddings
from langchain.tools import tool
from src.data_quality.checks import DQ_TOOLS  # This now imports [check_dataset_duplicates]
import chromadb
import os

# Define the path where your vector database will be stored
VECTOR_DB_PATH = "./chroma_db"

def build_index():
    """Parses DQ methods, embeds them, and stores them in ChromaDB."""
    print("Building RAG index...")

    # 1. Prepare data (extract docstrings and signatures from the single function)
    tool_docs = [
        str(tool(func)) # Converts the Python function into a LangChain Tool object
        for func in DQ_TOOLS
    ]

    # 2. Select Embeddings Model
    # Using a common, lightweight model for demonstration
    embeddings = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")

    # 3. Create or Connect to ChromaDB
    client = chromadb.PersistentClient(path=VECTOR_DB_PATH)

    # 4. Store documents (indexing)
    # Note: If this runs multiple times, you may want to delete the collection first
    # for a clean index, or implement an update strategy.
    Chroma.from_texts(
        texts=tool_docs,
        embedding=embeddings,
        client=client,
        collection_name="dq_methods"
    )

    print(f"Index built successfully in {VECTOR_DB_PATH} with {len(tool_docs)} method(s).")

if __name__ == "__main__":
    # Ensure this runs after updating checks.py
    build_index()
