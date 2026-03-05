# R-Centric-RAG-Pipeline
End-to-end RAG pipeline in R that converts PDFs and Excel files into a searchable knowledge base. PDFs use Mistral OCR to produce structured markdown; Excel rows are chunked with context. Chunks are embedded with text-embedding-3-small, stored in DuckDB, and queried via an LLM with citation-grounded answers.
