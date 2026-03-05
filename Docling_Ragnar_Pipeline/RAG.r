# =============================================================================
#  Retrieval-Augmented Generation) Pipeline in R using Docling and Ragnar
# =============================================================================
#  Description : Processes documents, chunks them using Docling, creates
#                embeddings, and enables hybrid search using the `ragnar`
#                package with a DuckDB vector store.
#
#  Prerequisites:
#    - R (>= 4.0) installed
#    - Python (>= 3.9) virtual environment with docling, markitdown, tiktoken
#    - OpenAI-compatible API access for embeddings
#    - The helper Python script `openai_chunking.py` (provided in this repo)
#
#  Refer to README.md for full setup instructions.
# =============================================================================


# INSTALL REQUIRED R PACKAGES
# install.packages("reticulate")
# install.packages("ragnar")
# install.packages("dplyr")
# install.packages("DBI")
# install.packages("duckdb")

# LOAD LIBRARIES

library(reticulate)
library(ragnar)
library(dplyr)
library(DBI)
library(duckdb)

# Run the following in your terminal first to set up Python:
#   mkdir ~/rag_project && cd ~/rag_project
#   python3 -m venv .venv
#   source .venv/bin/activate
#   pip install docling markitdown tiktoken


PYTHON_PATH <- "~/rag_project/.venv/bin/python"
CHUNKING_SCRIPT_PATH <- "~/rag_project/openai_chunking.py"
# --  Path to the input PDF document --
PDF_PATH <- "~/rag_project/your-document.pdf"
# --  Directory where the vector store database will be saved --
OUTPUT_DIR <- "~/rag_project"


#openai api configuration (if using OpenAI embeddings)
usethis::edit_r_environ() #Set the following environment variables in the file that opens Set the following environment variables in the file that opens:
# Inside the .Renviron file, add the following lines (replace with your actual values):
OPENAI_API_URL=https://api.openai.com/v1
OPENAI_API_KEY=your_openai_api_key_here

# CONFIGURE PYTHON ENVIRONMENT
Sys.setenv(RETICULATE_PYTHON = "PYTHON_PATH") # ex: "/rag_project/.venv/bin/python"
use_python("PYTHON_PATH", required = TRUE)

#verify configuration
#py_config()
#py_module_available("docling")

source_python("CHUNKING_SCRIPT_PATH")  #chunking script given in this repo as `openai_chunking.py` or 'chunking.py'

# =========================================================
# Extract Chunks from PDF
# =========================================================
pdf_path <- "/rag_project/your_data.pdf"
chunks <- get_chunks_with_tokens(pdf_path)
cat("Number of chunks:", length(chunks), "\n")


# =========================================================
# Create Ragnar Vector Store
# =========================================================

# Define database path
path <- paste0(
  OUTPUT_DIR, "/chunking_store_",
  format(Sys.time(), "%Y%m%d_%H%M%S"),
  ".ragnar.duckdb"
)
cat("Using new path:", path, "\n")

# Create ragnar store with OpenAI embeddings
store <- ragnar_store_create(
  path,
  embed = \(x) ragnar::embed_openai(
    x = x,
    model = "text-embedding-3-small",
    base_url = Sys.getenv("OPENAI_API_URL"),
    api_key = Sys.getenv("OPENAI_API_KEY"),
    user = Sys.getenv("USER"),
    batch_size = 20L  # Process 20 chunks at a time
  ),
  overwrite = TRUE,
  version = 1  # Version 1: accepts plain character vectors (version 2 for markdown chunk())
)

# ============================================================
# Insert Chunks and Build Vector Index
# ============================================================

# Insert chunks
ragnar_store_insert(store, chunks)
cat("✓ Inserted", length(chunks), "chunks\n")

# Build vector search index
ragnar_store_build_index(store)
cat("✓ Vector index built\n\n")

# ============================================================
#  Query the Vector Store
# ============================================================

# Define search query
query <- "What are the recent advancements in LLMs?"

# Retrieve relevant chunks based on semantic similarity
relevant_chunks <- ragnar_retrieve(store, query)

cat("✓ Retrieved", nrow(relevant_chunks), "relevant chunks\n\n")

# ============================================================
#  Inspect Results
# ============================================================

# Basic inspection
cat("--- Structure ---\n")
print(str(relevant_chunks))

cat("\n--- Top 3 Most Relevant Chunks ---\n")
top_3 <- relevant_chunks %>%
  arrange(cosine_distance) %>%  # Lower distance = more similar
  head(3) %>%
  select(origin, cosine_distance, text)

print(top_3)

cat("\n--- Top Result Full Text ---\n")
cat(relevant_chunks$text[1])

cat("\n\n--- Top 5 Summary ---\n")
top_5_summary <- relevant_chunks %>%
  arrange(cosine_distance) %>%
  head(5) %>%
  select(origin, cosine_distance) %>%
  mutate(text_preview = substr(relevant_chunks$text[1:5], 1, 100))

print(top_5_summary)
