# RAG Pipeline using docling chunkig and Ragnar

**Retrieval-Augmented Generation (RAG)** pipeline that integrates **R** and **Python** via `reticulate`. This pipeline processes documents, chunks them intelligently using `Docling`, creates embeddings, and enables Hybrid search using the `ragnar` package with DuckDB vector store.

------------------------------------------------------------------------

## Pipeline Overview

<p align="center">

<img src="Pipeline%20overview.png" alt="Pipeline Overview" style="width:100%; max-width:700px;"/>

</p>

**Pipeline Workflow:**

1.  **Ingest**: Load source documents (PDF, DOCX, MD, etc.)
2.  **Chunk**: Create text chunks using either a default tokenizer or OpenAI-guided chunking
3.  **Embed**: Generate embeddings (e.g., `text-embedding-3-small`) for semantic retrieval
4.  **Index**: Store chunks + embeddings in your vector + keyword index
5.  **Retrieve**: Hybrid search (keyword + semantic) returns top-k relevant chunks
6.  **Augment & Generate**: The LLM receives retrieved context and produces an answer 

**Core Technologies:** 
- **R**: `reticulate`, `ragnar`, `dplyr`, `DBI`, `duckdb`
- **Python**: `docling`, `tiktoken`, `markitdown`

------------------------------------------------------------------------

## Prerequisites

Before starting, ensure you have: - **R**: 4.0 or higher  

- **OpenAI API Key**: For embedding generation 

- **Linux/Unix environment**: Or WSL on Windows

------------------------------------------------------------------------

## Installation & Environment Setup

### Step 1: Create Python Environment

``` bash\terminal
# Create project directory
mkdir ~/rag_project
cd ~/rag_project

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install required Python packages
pip install docling markitdown tiktoken
```

### Step 2: Install R Packages

In RStudio open the RAG_Merlin_trial.r and run:

``` r
# Install required packages
install.packages("reticulate")
install.packages("ragnar") 
install.packages("dplyr")
install.packages("DBI")
install.packages("duckdb")
```

### Step 3: Configure Python Path in R

In your R script, set the Python environment path:

``` r
# Point to your virtual environment's Python
Sys.setenv(RETICULATE_PYTHON = "~/rag_project/.venv/bin/python")

# Load reticulate
library(reticulate)
use_python("~/rag_project/.venv/bin/python", required = TRUE)

# Verify configuration (optional)
py_config()
```

## Project Structure

``` text
rag_project/
├── .venv/                          # Python virtual environment
├── chunking.py                     # Basic chunking (HybridChunker)
├── openai_chunking.py              # Token-aware chunking (HybridChunker)
├── RAG_Pipline.r                   # Main R pipeline script
├── your-document.pdf               # Input PDF file
├── chunking_store_*.ragnar.duckdb  # Generated vector database (After running ragnar code)
└── README.md                       # This file
```

------------------------------------------------------------------------

## Chunking Options

### Option 1: `chunking.py` - Basic Chunking

-   Uses Docling's HybridChunker with default settings
-   Fast and simple
-   Good for general-purpose chunking

``` python
def get_chunks(source):
    from docling.document_converter import DocumentConverter
    from docling.chunking import HybridChunker
    
    converter = DocumentConverter()
    result = converter.convert(source)
    chunker = HybridChunker()
    return [chunk.text for chunk in chunker.chunk(result.document)]
```

### Option 2: `openai_chunking.py` - Token-Aware Chunking (Recommended)

-   Controls token count per chunk (max 8000 tokens)
-   Optimized for OpenAI embedding models
-   Better semantic coherence
-   Uses `tiktoken` for accurate token counting

``` python
def get_chunks_with_tokens(source, max_tokens=8000, model="text-embedding-3-small"):
    # Uses OpenAITokenizer for precise token control
    # See openai_chunking.py for full implementation
```

------------------------------------------------------------------------

## Running the Pipeline

### Complete R Script Workflow

``` r
library(reticulate)
library(ragnar)
library(dplyr)

# 1. Configure Python environment
Sys.setenv(RETICULATE_PYTHON = "~/rag_project/.venv/bin/python")
use_python("~/rag_project/.venv/bin/python", required = TRUE)

# 2. Source Python chunking function
source_python("~/rag_project/openai_chunking.py")

# 3. Extract chunks from PDF
pdf_path <- "~/rag_project/your-document.pdf"
chunks <- get_chunks_with_tokens(pdf_path)
cat("✓ Extracted", length(chunks), "chunks\n")

# 4. Create ragnar vector store
path <- paste0(
  "~/rag_project/chunking_store_",
  format(Sys.time(), "%Y%m%d_%H%M%S"),
  ".ragnar.duckdb"
)

store <- ragnar_store_create(
  path,
  embed = \(x) ragnar::embed_openai(
    x = x,
    model = "text-embedding-3-small",
    base_url = Sys.getenv("OPENAI_API_URL"),
    api_key = Sys.getenv("OPENAI_API_KEY"),
    user = Sys.getenv("USER"),
    batch_size = 20L
  ),
  overwrite = TRUE,
  version = 2
)

# 5. Insert chunks and build index
ragnar_store_insert(store, chunks)
ragnar_store_build_index(store)
cat("✓ Vector index built\n")

# 6. Query the store
query <- "What are the recent advancements in LLMs?"
results <- ragnar_retrieve(store, query)

# 7. View results
results %>%
  arrange(cosine_distance) %>%
  head(3) %>%
  select(origin, cosine_distance, text) %>%
  print()
```

------------------------------------------------------------------------

## Understanding the Results

The `ragnar_retrieve()` function returns a data frame with: - **`text`**: The chunk content - **`cosine_distance`**: Similarity score (lower = more similar) - **`origin`**: Source document reference

``` r
# Get top 5 most relevant chunks
top_chunks <- results %>%
  arrange(cosine_distance) %>%
  head(5)

# Preview each chunk
for(i in 1:nrow(top_chunks)) {
  cat("\n--- Chunk", i, "---\n")
  cat("Distance:", top_chunks$cosine_distance[i], "\n")
  cat(substr(top_chunks$text[i], 1, 200), "...\n")
}
```

------------------------------------------------------------------------

## Troubleshooting

### Issue 1: Reticulate Uses Wrong Python

**Symptom**: R loads system Python instead of your virtual environment

**Solution**:

``` bash
# Remove broken environment
rm -rf ~/rag_project/.venv

# Recreate from scratch
python3 -m venv ~/rag_project/.venv
source ~/rag_project/.venv/bin/activate
pip install docling markitdown tiktoken 
```

Then in R:

``` r
Sys.setenv(RETICULATE_PYTHON = "~/rag_project/.venv/bin/python")
use_python("~/rag_project/.venv/bin/python", required = TRUE)
```

### Issue 2: OpenAI API Errors

**Common Error**:

```         
Error in `req_perform()`:
! HTTP 401 Unauthorized.
• OAuth error: invalid_token - Unable to find the access token in persistent storage.
```

**Solution - Check API Credentials**:



``` r
# Set Merlin  credentials
Sys.setenv(OPENAI_API_KEY = "your-api-key")
Sys.setenv(OPENAI_API_URL = "your-api-url")
```

### Issue 3: Module Import Errors

**Verify Python modules**:

``` bash
source ~/rag_project/.venv/bin/activate
python -c "import docling; print('✓ docling installed')"
python -c "import tiktoken; print('✓ tiktoken installed')"
```

------------------------------------------------------------------------

## Acknowledgments

-   **Docling**: Document parsing and intelligent chunking
-   **Ragnar**: R wrapper for vector search with DuckDB
-   **OpenAI**: Embedding models (`text-embedding-3-small`)
-   **Reticulate**: Seamless R-Python integration

------------------------------------------------------------------------


The pipeline was tested with smaller files, and the chunks were retrieved correctly. For handling larger files, a different pipeline was created that supports both PDF and Excel processing. Please feel free to reach out if you have any doubts.
