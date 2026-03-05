################################################################################
#                    PDF & EXCEL OCR + RAG PIPELINE                            #
################################################################################
#
# WORKFLOW:
#   1. PDF Processing: Convert PDF to markdown with image annotations via OCR
#   2. Excel Processing: Process ADS Excel files into structured chunks
#   3. Chunking: Split documents into manageable chunks (done by markdown_chunk())
#   4. RAG Store: Create unified vector embeddings for both pdf and excel
#   5. Retrieval: Query the store to find relevant document sections
#
## PREREQUISITES:
#   You need API keys for two services: 1. **Mistral AI** (for OCR) , 2. **OpenAI** (for embeddings + LLM generation)
################################################################################

### uncomment for installtion ######
#Package installation
# install.packages(c(
#   "base64enc",
#   "pdftools",
#   "DBI",
#   "duckdb",
#   "future",
#   "future.apply",
#   "progressr",
#   "readxl",
#   "stringr",
#   "usethis",
#   "ellmer",
#   "httr",
#   "ragnar"
# ))


# Load required libraries
library(httr2)
library(base64enc)
library(pdftools)
library(ragnar)
library(DBI)
library(duckdb)
library(future)
library(future.apply)
library(progressr)
library(readxl)
library(stringr)

################################################################################
#                          CONFIGURATION                                       #
################################################################################
#usethis::edit_r_environ()

# To set environment variables, run: usethis::edit_r_environ()
# Then add these lines (replace with your actual keys):
#
#   MISTRAL_API_KEY=your_mistral_api_key_here
#   MISTRAL_BASE_URL=https://api.mistral.ai/v1
#   OPENAI_API_KEY=your_openai_api_key_here
#   OPENAI_BASE_URL=https://api.openai.com/v1
#   OPENAI_CHAT_MODEL=gpt-5-mini
#
# Save the file and restart R.
# --- Mistral API (used for OCR) ---
MISTRAL_API_KEY  <- Sys.getenv("MISTRAL_API_KEY")
MISTRAL_BASE_URL <- Sys.getenv("MISTRAL_BASE_URL", "https://api.mistral.ai/v1")

# --- OpenAI API (used for embeddings + LLM chat) ---
OPENAI_API_KEY  <- Sys.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL <- Sys.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_CHAT_MODEL <- Sys.getenv("OPENAI_CHAT_MODEL", "gpt-5-mini")

# Working directory - Processing files has to be in working dir (pdf or excel)
WORKING_DIR <- getwd()

# --- Discover PDF files ---
PDF_FILES <- list.files(
  path = WORKING_DIR,
  pattern = "\\.pdf$",
  full.names = TRUE,
  ignore.case = TRUE
)
NUM_PDF_FILES <- length(PDF_FILES)

# --- Discover Excel files (.xlsx, .xlsm, .xls) ---
EXCEL_FILES <- list.files(
  path = WORKING_DIR,
  pattern = "\\.(xlsx|xlsm|xls)$",
  full.names = TRUE,
  ignore.case = TRUE
)
NUM_EXCEL_FILES <- length(EXCEL_FILES)

# Validate that at least one file exists
if (NUM_PDF_FILES == 0 && NUM_EXCEL_FILES == 0) {
  stop("No PDF or Excel files found in working directory: ", WORKING_DIR)
}

cat(sprintf("\nFound %d PDF file(s) and %d Excel file(s) in: %s\n",
            NUM_PDF_FILES, NUM_EXCEL_FILES, WORKING_DIR))

# Generate output markdown filenames for PDFs
OUTPUT_MARKDOWNS <- gsub("\\.pdf$", "_output.md", PDF_FILES, ignore.case = TRUE)

# Dynamically assign PDF variables: INPUT_PDF_1, INPUT_PDF_2, etc.
for (i in seq_along(PDF_FILES)) {
  assign(paste0("INPUT_PDF_", i), PDF_FILES[i], envir = .GlobalEnv)
}

# Dynamically assign output markdown variables
for (i in seq_along(OUTPUT_MARKDOWNS)) {
  assign(paste0("OUTPUT_MARKDOWN_", i), OUTPUT_MARKDOWNS[i], envir = .GlobalEnv)
}

# RAG database path (single unified database for PDFs + Excel)
RAG_DATABASE <- file.path(WORKING_DIR, "rag_store.duckdb")

# Processing parameters
MAX_PAGES_PER_CHUNK <- 28  # Mistral OCR API limit per call

#' Create Embedding Function for RAG Store
#'
#' Uses OpenAI-compatible text-embedding-3-small model.
#' @param x Character vector of texts to embed
#' @return Matrix of embeddings
embed_function <- function(x) {
  ragnar::embed_openai(
    x = x,
    model = "openai-text-embedding-3-small",
    base_url = Sys.getenv("OPENAI_BASE_URL"),
    api_key  = Sys.getenv("OPENAI_API_KEY"),
    user     = Sys.getenv("USER"),
    batch_size = 100L  #adjust batch size to check the speed
  )
}


# detect available cores
available_cores <- future::availableCores()
# Set CPU workers based on detected cores
cpu <- min(available_cores - 1, 32)   # capped at the recommended upper limit

#sheet selection
desired_sheets <- c('Sheet1', 'Sheet2')  #replace with your sheet names or set to NULL to process all sheets
#desired_sheets <- NULL
################################################################################
#                       SECTION 1: PDF CHUNKING                                #
################################################################################

#' Prepare PDF for OCR Processing
#'
#' Splits a PDF into chunks of at most `max_pages` pages each.
#' If the PDF has <= max_pages, no splitting is needed.
#'
#' @param file_path Path to the input PDF file
#' @param max_pages Maximum pages per chunk (default: 28)
#' @return List of chunk metadata including file paths and page ranges
prepare_pdf_chunks <- function(file_path, max_pages = 28) {
  total_pages <- pdf_info(file_path)$pages
  num_chunks <- ceiling(total_pages / max_pages)
  chunks <- list()

  for (i in 1:num_chunks) {
    start_page <- (i - 1) * max_pages + 1
    end_page <- min(i * max_pages, total_pages)

    if (num_chunks == 1) {
      # Single chunk — use original file directly
      chunks[[i]] <- list(
        file_path = file_path,
        is_temp = FALSE,
        chunk_num = i,
        total_chunks = num_chunks,
        pages_to_process = NULL,
        start_page = start_page,
        end_page = end_page
      )
    } else {
      # Multiple chunks — extract page range into temp file
      temp_file <- tempfile(fileext = ".pdf")
      pdf_subset(file_path, pages = start_page:end_page, output = temp_file)

      chunks[[i]] <- list(
        file_path = temp_file,
        is_temp = TRUE,
        chunk_num = i,
        total_chunks = num_chunks,
        pages_to_process = NULL,
        start_page = start_page,
        end_page = end_page
      )
    }
  }

  return(chunks)
}

################################################################################
#                   SECTION 2: ENCODING & MARKDOWN UTILITIES                   #
################################################################################

#' Encode PDF Document to Base64
#'
#' @param file_path Path to PDF file
#' @return Base64-encoded string
encode_document <- function(file_path) {
  base64encode(file_path)
}

#' Replace Image Placeholders with Annotations in Markdown
#'
#' Replaces ![img_name](img_name) patterns with bold annotation text.
#'
#' @param markdown_str Original markdown string
#' @param images_dict Named list where each entry has an "annotation" field
#' @return Markdown with images replaced by annotations
replace_images_in_markdown_annotated <- function(markdown_str, images_dict) {
  for (img_name in names(images_dict)) {
    annotation <- images_dict[[img_name]][["annotation"]]
    pattern <- paste0("!\\[", img_name, "\\]\\(", img_name, "\\)")
    replacement <- paste0("**", annotation, "**")
    markdown_str <- gsub(pattern, replacement, markdown_str, fixed = FALSE)
  }
  return(markdown_str)
}

#' Combine Page-Level Markdown with Annotations
#'
#' Assembles a complete markdown document from OCR API response,
#' replacing image references with their textual annotations.
#'
#' @param ocr_response JSON response from OCR API
#' @param include_document_annotation Whether to include document-level annotation
#' @return Complete markdown string
get_combined_markdown_annotated <- function(ocr_response, include_document_annotation = TRUE) {
  markdowns <- list()

  # Add document-level annotation if available and requested
  if (include_document_annotation && !is.null(ocr_response$document_annotation)) {
    markdowns <- list(paste0("**", ocr_response$document_annotation, "**"))
  }

  # Process each page
  for (page in ocr_response$pages) {
    image_data <- list()
    for (img in page$images) {
      image_data[[img$id]] <- list(annotation = img$image_annotation)
    }
    annotated_markdown <- replace_images_in_markdown_annotated(page$markdown, image_data)
    markdowns <- append(markdowns, annotated_markdown)
  }

  return(paste(markdowns, collapse = "\n\n"))
}

################################################################################
#                     SECTION 3: OCR API PROCESSING                            #
################################################################################

#' Process Single PDF Chunk via Mistral OCR API
#'
#' Sends a base64-encoded PDF chunk to the Mistral OCR endpoint
#' and returns structured JSON with markdown + image annotations.
#'
#' @param chunk Chunk metadata from prepare_pdf_chunks()
#' @param base64_data Base64-encoded PDF data
#' @return JSON response from API, or NULL on error
process_chunk <- function(chunk, base64_data) {
  payload <- list(
    model = "mistral-ocr",
    document = list(
      type = "document_url",
      document_url = paste0("data:application/pdf;base64,", base64_data)
    ),
    bbox_annotation_format = list(
      type = "json_schema",
      json_schema = list(
        schema = list(
          properties = list(
            document_type = list(
              title = "Document_Type",
              description = "The type of the image.",
              type = "string"
            ),
            short_description = list(
              title = "Short_Description",
              description = "A description in English describing the image.",
              type = "string"
            ),
            summary = list(
              title = "Summary",
              description = "Summarize the image.", #rewrite it for better summary
              type = "string"
            )
          ),
          required = c("document_type", "short_description", "summary"),
          title = "BBOXAnnotation",
          type = "object",
          additionalProperties = FALSE
        ),
        name = "document_annotation",
        strict = TRUE
      )
    ),
    include_image_base64 = TRUE,
    table_format = "html"
  )

  if (!is.null(chunk$pages_to_process)) {
    payload$pages <- chunk$pages_to_process
  }

  response <- request(MISTRAL_BASE_URL) |>
    req_url_path_append("ocr") |>
    req_headers(
      Authorization = paste("Bearer", MISTRAL_API_KEY),
      `Content-Type` = "application/json"
    ) |>
    req_body_json(payload) |>
    req_timeout(300) |>
    req_error(is_error = function(resp) FALSE) |>
    req_perform()

  status <- resp_status(response)

  if (status != 200) {
    cat("Error: API request failed with status", status, "\n")
    tryCatch({
      body <- resp_body_string(response)
      cat("Response body:", substr(body, 1, 500), "\n")
    }, error = function(e) NULL)
    return(NULL)
  }

  return(resp_body_json(response))
}

################################################################################
#               SECTION 4: EXCEL PROCESSING FUNCTIONS                          #
################################################################################

#' Process ADS Excel Files into Ragnar-Compatible Chunks
#'
#' Reads specified sheets from an Excel file, converts each row into a
#' structured text chunk with context hierarchy, and computes character
#' positions within a full concatenated document.
#'
#' @param file_path Path to the Excel file (.xlsx, .xlsm, .xls)
#' @param sheets_to_process Character vector of sheet names to process.
#'        If NULL, processes all sheets.
#' @param verbose Show progress messages (default: FALSE)
#' @return List with:
#'   - `document`: Full concatenated text of all chunks
#'   - `chunks`: List of chunk records (start, end, context, text)
process_ads_excel_file <- function(file_path,
                                   sheets_to_process = NULL,
                                   verbose = FALSE) {

  # Collectors for chunk texts and contexts

  chunk_texts <- character()
  chunk_contexts <- character()

  # Step 1: Read available sheet names
  if (verbose) message("Reading sheet names from: ", basename(file_path))
  sheet_names <- readxl::excel_sheets(file_path)

  # Step 2: Determine which sheets to process
  if (is.null(sheets_to_process)) {
    sheets_to_process <- sheet_names
    if (verbose) message("Processing all ", length(sheets_to_process), " sheets")
  } else {
    sheets_to_process <- sheets_to_process[sheets_to_process %in% sheet_names]
    if (verbose) message("Processing ", length(sheets_to_process), " matching sheets")
  }

  if (length(sheets_to_process) == 0) {
    warning("No matching sheets found in: ", basename(file_path))
    return(list(document = "", chunks = list()))
  }

  # Step 3: Required columns for excel file
  required_cols <- c("Variable Name", "Variable Label", "Source / Derivation")

  # Step 4: Process each sheet — build chunk text and context per row
  for (sheet in sheets_to_process) {
    if (verbose) message("\n--- Processing sheet: ", sheet, " ---")

    tryCatch({
      # Read sheet data
      suppressMessages({
        data <- readxl::read_excel(
          file_path,
          sheet = sheet,
          .name_repair = "minimal"
        )
      })

      # Skip empty sheets
      if (nrow(data) == 0) {
        if (verbose) message("  -> Sheet is empty, skipping")
        next
      }

      if (verbose) message("  -> Loaded ", nrow(data), " rows")

      # Validate required columns exist
      missing_cols <- required_cols[!required_cols %in% names(data)]
      if (length(missing_cols) > 0) {
        message("  -> Warning: Missing columns in '", sheet, "': ",
                paste(missing_cols, collapse = ", "), " - Skipping")
        next
      }

      # Remove rows where ALL values are NA
      filtered <- data[rowSums(is.na(data)) != ncol(data), ]

      if (nrow(filtered) == 0) {
        if (verbose) message("  -> No valid data after filtering, skipping")
        next
      }

      if (verbose) message("  -> Creating chunks for ", nrow(filtered), " rows")

      # Step 5: Build one chunk per row with all columns as key-value pairs
      for (i in seq_len(nrow(filtered))) {
        row_data <- filtered[i, , drop = FALSE]

        # Context: hierarchical path for retrieval
        variable_name <- row_data[["Variable Name"]]
        variable_name <- ifelse(is.na(variable_name), "Unknown", as.character(variable_name))
        context <- paste0("ADS Plan > ", sheet, " > Variable: ", variable_name)

        # Build chunk text: one line per column
        chunk_parts <- character()
        for (col_name in names(row_data)) {
          col_value <- row_data[[col_name]]
          col_value <- ifelse(is.na(col_value), "", as.character(col_value))
          clean_name <- tolower(gsub(" ", "_", col_name))
          chunk_parts <- c(chunk_parts, paste0(clean_name, ": ", col_value))
        }

        chunk_text <- paste(chunk_parts, collapse = "\n")

        chunk_texts <- c(chunk_texts, chunk_text)
        chunk_contexts <- c(chunk_contexts, context)
      }

      if (verbose) message("  -> Created ", nrow(filtered), " chunks for sheet '", sheet, "'")

      # Free memory
      rm(data, filtered)
      gc(verbose = FALSE)

    }, error = function(e) {
      message("  -> ERROR processing sheet '", sheet, "': ", e$message)
    })
  }

  # Step 6: Concatenate all chunk texts into a single full document
  separator <- "\n\n"
  full_document <- paste(chunk_texts, collapse = separator)

  if (verbose) {
    message("\n=== Full document created ===")
    message("Document length: ", nchar(full_document), " characters")
    message("Total chunks: ", length(chunk_texts))
  }

  # Step 7: Compute start/end character positions for each chunk in the document
  chunks <- list()
  for (i in seq_along(chunk_texts)) {
    positions <- str_locate(full_document, fixed(chunk_texts[i]))
    chunks[[i]] <- list(
      start   = positions[1, "start"],
      end     = positions[1, "end"],
      context = chunk_contexts[i],
      text    = chunk_texts[i]
    )
  }

  if (verbose) {
    message("\n=== Excel Processing Complete ===")
    message("Total chunks created: ", length(chunks))
  }

  return(list(
    document = full_document,
    chunks   = chunks
  ))
}

#' Process a Single Sheet from an Excel File
#'
#' Reads one sheet, validates required columns, and returns chunk texts + contexts.
#' Designed to run independently inside a parallel worker.
#'
#' @param file_path Path to the Excel file
#' @param sheet Name of the sheet to process
#' @param required_cols Character vector of required column names
#' @param verbose Show progress messages
#' @return List with `chunk_texts` and `chunk_contexts` character vectors
process_single_sheet <- function(file_path, sheet,
                                 required_cols = c("Variable Name"), #"Variable Label", "Source / Derivation"),  #mention columns based on your requriements
                                 verbose = FALSE) {

  chunk_texts <- character()
  chunk_contexts <- character()

  if (verbose) message("  Processing sheet: ", sheet)

  # Read sheet data
  suppressMessages({
    data <- readxl::read_excel(
      file_path,
      sheet = sheet,
      .name_repair = "minimal"
    )
  })

  # Skip empty sheets
  if (nrow(data) == 0) {
    if (verbose) message("  -> Sheet '", sheet, "' is empty, skipping")
    return(list(chunk_texts = chunk_texts, chunk_contexts = chunk_contexts, sheet = sheet))
  }

  # Validate required columns
  missing_cols <- required_cols[!required_cols %in% names(data)]
  if (length(missing_cols) > 0) {
    if (verbose) message("  -> Missing columns in '", sheet, "': ",
                         paste(missing_cols, collapse = ", "), " — skipping")
    return(list(chunk_texts = chunk_texts, chunk_contexts = chunk_contexts, sheet = sheet))
  }

  # Remove fully-NA rows
  filtered <- data[rowSums(is.na(data)) != ncol(data), ]

  if (nrow(filtered) == 0) {
    if (verbose) message("  -> No valid data in '", sheet, "' after filtering")
    return(list(chunk_texts = chunk_texts, chunk_contexts = chunk_contexts, sheet = sheet))
  }

  # Build one chunk per row
  for (i in seq_len(nrow(filtered))) {
    row_data <- filtered[i, , drop = FALSE]

    variable_name <- row_data[["Variable Name"]]
    variable_name <- ifelse(is.na(variable_name), "Unknown", as.character(variable_name))
    context <- paste0("ADS Plan > ", sheet, " > Variable: ", variable_name)

    chunk_parts <- character()
    for (col_name in names(row_data)) {
      col_value <- row_data[[col_name]]
      col_value <- ifelse(is.na(col_value), "", as.character(col_value))
      clean_name <- tolower(gsub(" ", "_", col_name))
      chunk_parts <- c(chunk_parts, paste0(clean_name, ": ", col_value))
    }

    chunk_text <- paste(chunk_parts, collapse = "\n")
    chunk_texts <- c(chunk_texts, chunk_text)
    chunk_contexts <- c(chunk_contexts, context)
  }

  if (verbose) message("  -> Created ", length(chunk_texts), " chunks from sheet '", sheet, "'")

  return(list(
    chunk_texts    = chunk_texts,
    chunk_contexts = chunk_contexts,
    sheet          = sheet,
    num_rows       = nrow(filtered)
  ))
}

#' Convert Excel Chunks List to Data Frame
#'
#' @param chunks List of chunk records from process_ads_excel_file()
#' @return Data frame with columns: start, end, context, text
chunks_to_df <- function(chunks) {
  if (length(chunks) == 0) {
    return(data.frame(
      start = integer(), end = integer(),
      context = character(), text = character(),
      stringsAsFactors = FALSE
    ))
  }

  data.frame(
    start = vapply(chunks, function(x) as.integer(x$start), integer(1)),
    end   = vapply(chunks, function(x) as.integer(x$end), integer(1)),
    context = vapply(chunks, function(x) x$context, character(1)),
    text    = vapply(chunks, function(x) x$text, character(1)),
    stringsAsFactors = FALSE
  )
}

################################################################################
#                   SECTION 5: MAIN OCR PROCESSING PIPELINE (PDFs)             #
################################################################################

all_pdf_results <- list()

if (NUM_PDF_FILES > 0) {
  cat("\n========== PHASE 1: PDF OCR Processing ==========\n")

  for (pdf_index in seq_along(PDF_FILES)) {
    pdf_file <- PDF_FILES[pdf_index]
    output_markdown <- OUTPUT_MARKDOWNS[pdf_index]

    cat(sprintf("\nProcessing PDF %d/%d: %s\n",
                pdf_index, NUM_PDF_FILES, basename(pdf_file)))

    # Prepare PDF chunks (split if > MAX_PAGES_PER_CHUNK pages)
    pdf_chunks <- prepare_pdf_chunks(pdf_file, max_pages = MAX_PAGES_PER_CHUNK)

    # Configure parallel processing (max 2 workers)
    num_workers <- min(cpu, length(pdf_chunks))
    plan(multisession, workers = num_workers)

    # Process chunks in parallel via Mistral OCR API
    results <- future_lapply(
      seq_along(pdf_chunks),
      function(i) {
        # library(base64enc)
        # library(httr2)

        chunk <- pdf_chunks[[i]]

        result <- list(
          index = i,
          chunk = chunk,
          markdown = NULL,
          success = FALSE,
          start_time = Sys.time()
        )

        tryCatch({
          base64_data <- encode_document(chunk$file_path)
          response_json <- process_chunk(chunk, base64_data)

          if (!is.null(response_json)) {
            include_doc_annotation <- (i == 1)
            chunk_md <- get_combined_markdown_annotated(response_json, include_doc_annotation)
            result$markdown <- chunk_md
            result$success <- TRUE
          }

          # Clean up temporary files
          if (chunk$is_temp && file.exists(chunk$file_path)) {
            unlink(chunk$file_path)
          }

        }, error = function(e) {
          result$error_message <<- as.character(e)
        })

        result$end_time <- Sys.time()
        return(result)
      },
      future.seed = TRUE,
      future.globals = structure(TRUE, add = c(
        "encode_document",
        "process_chunk",
        "get_combined_markdown_annotated",
        "replace_images_in_markdown_annotated",
        "BASE_URL",
        "API_KEY"
      ))
    )

    # Combine results in order
    all_markdown <- list()
    for (result in results) {
      if (result$success) {
        all_markdown[[result$index]] <- result$markdown
      }
    }

    # Save combined markdown to file
    if (length(all_markdown) > 0) {
      final_markdown <- paste(all_markdown, collapse = "\n\n---\n\n")
      writeLines(final_markdown, output_markdown)
      cat(sprintf("  -> Markdown saved to: %s\n", output_markdown))

      all_pdf_results[[pdf_index]] <- list(
        pdf_file = pdf_file,
        output_markdown = output_markdown,
        final_markdown = final_markdown,
        num_chunks = length(pdf_chunks),
        success = TRUE
      )
    } else {
      cat("  -> Error: No successful chunks to combine\n")
      all_pdf_results[[pdf_index]] <- list(
        pdf_file = pdf_file,
        output_markdown = output_markdown,
        final_markdown = NULL,
        num_chunks = length(pdf_chunks),
        success = FALSE
      )
    }

    # Reset to sequential after each PDF
    plan(sequential)
  }

  cat(sprintf("\nPDF processing complete: %d PDF(s) processed\n", NUM_PDF_FILES))
} else {
  cat("\nNo PDF files found — skipping PDF processing.\n")
}

################################################################################
#           SECTION 6: EXCEL PROCESSING PIPELINE            #
################################################################################

all_excel_results <- list()

if (NUM_EXCEL_FILES > 0) {
  cat("\n========== PHASE 2: Excel Processing (Parallel by Sheet) ==========\n")

  for (excel_index in seq_along(EXCEL_FILES)) {
    excel_file <- EXCEL_FILES[excel_index]

    cat(sprintf("\nProcessing Excel %d/%d: %s\n",
                excel_index, NUM_EXCEL_FILES, basename(excel_file)))

    tryCatch({
      # --- Step 1: Discover sheets ---
      all_sheet_names <- readxl::excel_sheets(excel_file)

      # Filter to desired sheets (set to NULL to process ALL sheets)
      #desired_sheets <- NULL

      if (!is.null(desired_sheets)) {
        sheets_to_process <- all_sheet_names[all_sheet_names %in% desired_sheets]
      } else {
        sheets_to_process <- all_sheet_names
      }

      if (length(sheets_to_process) == 0) {
        cat(sprintf("  -> No matching sheets found in %s, skipping\n",
                    basename(excel_file)))
        all_excel_results[[excel_index]] <- list(
          excel_file = excel_file, document = NULL,
          chunks = list(), num_chunks = 0, success = FALSE
        )
        next
      }

      cat(sprintf("  -> Found %d sheet(s) to process: %s\n",
                  length(sheets_to_process),
                  paste(sheets_to_process, collapse = ", ")))

      # --- Step 2: Process sheets in parallel ---
      num_sheet_workers <- min(cpu, length(sheets_to_process))
      plan(multisession, workers = num_sheet_workers)

      cat(sprintf("  -> Launching %d parallel workers for %d sheet(s)\n",
                  num_sheet_workers, length(sheets_to_process)))

      sheet_results <- future_lapply(
        sheets_to_process,
        function(sheet_name) {
          library(readxl)

          process_single_sheet(
            file_path     = excel_file,
            sheet         = sheet_name,
            required_cols = c("Variable Name", "Variable Label", "Source / Derivation"),
            verbose       = TRUE
          )
        },
        future.seed = TRUE,
        future.globals = structure(TRUE, add = c(
          "process_single_sheet",
          "excel_file"
        ))
      )

      # Reset to sequential
      plan(sequential)

      # --- Step 3: Combine results from all sheets ---
      combined_texts    <- character()
      combined_contexts <- character()

      for (sr in sheet_results) {
        if (length(sr$chunk_texts) > 0) {
          combined_texts    <- c(combined_texts,    sr$chunk_texts)
          combined_contexts <- c(combined_contexts, sr$chunk_contexts)
          cat(sprintf("    Sheet '%s': %d chunks\n", sr$sheet, length(sr$chunk_texts)))
        } else {
          cat(sprintf("    Sheet '%s': 0 chunks (skipped or empty)\n", sr$sheet))
        }
      }

      # --- Step 4: Build full document and chunk positions ---
      if (length(combined_texts) > 0) {
        separator <- "\n\n"
        full_document <- paste(combined_texts, collapse = separator)

        chunks <- list()
        current_pos <- 1L
        for (i in seq_along(combined_texts)) {
          text_len <- nchar(combined_texts[i])
          chunks[[i]] <- list(
            start   = current_pos,
            end     = current_pos + text_len - 1L,
            context = combined_contexts[i],
            text    = combined_texts[i]
          )
          current_pos <- current_pos + text_len + nchar(separator)
        }

        all_excel_results[[excel_index]] <- list(
          excel_file = excel_file,
          document   = full_document,
          chunks     = chunks,
          num_chunks = length(chunks),
          success    = TRUE
        )

        cat(sprintf("  -> Total: %d chunks from %s\n",
                    length(chunks), basename(excel_file)))
      } else {
        cat(sprintf("  -> No chunks created from %s\n", basename(excel_file)))
        all_excel_results[[excel_index]] <- list(
          excel_file = excel_file, document = NULL,
          chunks = list(), num_chunks = 0, success = FALSE
        )
      }

    }, error = function(e) {
      cat(sprintf("  -> ERROR processing %s: %s\n",
                  basename(excel_file), e$message))
      plan(sequential)  # Ensure cleanup on error
      all_excel_results[[excel_index]] <<- list(
        excel_file = excel_file, document = NULL,
        chunks = list(), num_chunks = 0, success = FALSE
      )
    })
  }

  cat(sprintf("\nExcel processing complete: %d file(s) processed\n", NUM_EXCEL_FILES))
} else {
  cat("\nNo Excel files found — skipping Excel processing.\n")
}

################################################################################
#        SECTION 7: UNIFIED RAG STORE CREATION & EMBEDDING                     #
#        (with duplicate handling)                                             #
################################################################################

cat("\n========== PHASE 3: Building Unified RAG Database ==========\n")

total_chunks <- 0
store <- NULL
doc_counter <- 0

# -------------------------------------------------------------------------
# Helper: Remove duplicate chunks before insertion
# Duplicates cause PRIMARY KEY violations in DuckDB
# -------------------------------------------------------------------------
deduplicate_chunks <- function(chunks_df) {
  if (nrow(chunks_df) == 0) return(chunks_df)

  # Remove rows with identical text content
  if ("text" %in% names(chunks_df)) {
    before <- nrow(chunks_df)
    chunks_df <- chunks_df[!duplicated(chunks_df$text), ]
    after <- nrow(chunks_df)
    if (before != after) {
      cat(sprintf("  -> Removed %d duplicate chunks (%d -> %d)\n",
                  before - after, before, after))
    }
  }

  # Remove rows with identical (start, end) positions
  if (all(c("start", "end") %in% names(chunks_df))) {
    before <- nrow(chunks_df)
    chunks_df <- chunks_df[!duplicated(chunks_df[, c("start", "end")]), ]
    after <- nrow(chunks_df)
    if (before != after) {
      cat(sprintf("  -> Removed %d positional duplicates (%d -> %d)\n",
                  before - after, before, after))
    }
  }

  return(chunks_df)
}

# -------------------------------------------------------------------------
# Create the store ONCE before any inserts
# -------------------------------------------------------------------------
store <- ragnar_store_create(
  RAG_DATABASE,
  embed = embed_function,
  overwrite = TRUE  # Fresh database each run — avoids stale duplicates
)

# --- Insert PDF documents (one at a time to avoid key collisions) ---
for (pdf_index in seq_along(all_pdf_results)) {
  result <- all_pdf_results[[pdf_index]]
  if (is.null(result) || !result$success) next

  doc_counter <- doc_counter + 1
  cat(sprintf("\nInserting PDF %d: %s\n",
              doc_counter, basename(result$pdf_file)))

  tryCatch({
    # Chunk the markdown using ragnar's built-in chunker
    chunks <- ragnar::markdown_chunk(result$final_markdown)
    chunks$origin <- basename(result$pdf_file)

    # Deduplicate before insert
    chunks <- deduplicate_chunks(chunks)

    if (nrow(chunks) > 0) {
      ragnar_store_insert(store, chunks)
      total_chunks <- total_chunks + nrow(chunks)
      cat(sprintf("  -> Inserted %d chunks\n", nrow(chunks)))
    }

  }, error = function(e) {
    cat(sprintf("  -> ERROR inserting PDF '%s': %s\n",
                basename(result$pdf_file), e$message))
  })
}

# --- Insert Excel documents (one at a time, with deduplication) ---
for (excel_index in seq_along(all_excel_results)) {
  result <- all_excel_results[[excel_index]]
  if (is.null(result) || !result$success) next

  doc_counter <- doc_counter + 1
  cat(sprintf("\nInserting Excel %d: %s\n",
              doc_counter, basename(result$excel_file)))

  tryCatch({
    # Convert chunks to data frame and deduplicate
    df <- chunks_to_df(result$chunks)
    df <- deduplicate_chunks(df)

    if (nrow(df) == 0) {
      cat("  -> No chunks after deduplication, skipping\n")
      next
    }

    # Rebuild chunk positions after deduplication
    # Recalculate start/end from the full document to avoid collisions
    unique_texts <- df$text
    unique_contexts <- df$context

    # Reconstruct document from deduplicated chunks only
    separator <- "\n\n"
    clean_document <- paste(unique_texts, collapse = separator)

    # Recompute positions in the clean document
    new_starts <- integer(length(unique_texts))
    new_ends <- integer(length(unique_texts))
    current_pos <- 1L

    for (i in seq_along(unique_texts)) {
      text_len <- nchar(unique_texts[i])
      new_starts[i] <- current_pos
      new_ends[i] <- current_pos + text_len - 1L
      current_pos <- current_pos + text_len + nchar(separator)
    }

    # Build Ragnar-compatible object with clean positions
    chunk_tbl <- tibble::tibble(
      start   = new_starts,
      end     = new_ends,
      context = unique_contexts
    )

    doc <- ragnar::MarkdownDocument(
      clean_document,
      origin = basename(result$excel_file)
    )
    excel_chunks <- ragnar::MarkdownDocumentChunks(chunk_tbl, document = doc)

    ragnar_store_insert(store, excel_chunks)
    total_chunks <- total_chunks + nrow(df)
    cat(sprintf("  -> Inserted %d chunks\n", nrow(df)))

  }, error = function(e) {
    cat(sprintf("  -> ERROR inserting Excel '%s': %s\n",
                basename(result$excel_file), e$message))
  })
}

# --- Build unified search index ---
if (total_chunks > 0) {
  ragnar_store_build_index(store)

  num_successful_pdfs <- sum(vapply(all_pdf_results,
                                    function(x) if (!is.null(x)) x$success else FALSE, logical(1)))
  num_successful_excels <- sum(vapply(all_excel_results,
                                      function(x) if (!is.null(x)) x$success else FALSE, logical(1)))

  cat(sprintf(
    "\n=== RAG Database Ready ===\n  Path: %s\n  Total chunks: %d\n  PDFs: %d\n  Excels: %d\n",
    RAG_DATABASE, total_chunks, num_successful_pdfs, num_successful_excels
  ))
} else {
  stop("No documents were successfully processed. Cannot build RAG database.")
}

################################################################################
#              SECTION 8: DATABASE INSPECTION & VERIFICATION                   #
################################################################################

# Uncomment this section to verify database structure and content
# con <- DBI::dbConnect(duckdb::duckdb(), dbdir = RAG_DATABASE,
#                       read_only = TRUE, array = "matrix")
# tables <- dbListTables(con)
# cat("Tables:", paste(tables, collapse = ", "), "\n")
# chunks_tbl <- dbReadTable(con, "chunks")
# cat("\nDocuments in database:\n")
# print(table(chunks_tbl$origin))
# cat("\nTotal chunks:", nrow(chunks_tbl), "\n")
# dbDisconnect(con, shutdown = TRUE)

################################################################################
#                SECTION 9: SEMANTIC RETRIEVAL QUERIES                         #
################################################################################

cat("\n========== PHASE 4: RAG Retrieval Ready ==========\n")

# Connect to the unified RAG store
store_location <- ragnar_store_connect(RAG_DATABASE)

# System prompt: strictly grounded in retrieved content
system_prompt <- stringr::str_squish(
  "You are an expert assistant whose answers must come strictly and exclusively
from the Retrieval-Augmented Generation (RAG) knowledge base.

### Core Rules
1. **You must retrieve relevant passages from the vector database before answering.**
2. **You are not allowed to use outside knowledge.**
    If the answer is not found in the retrieved documents, say:
     The requested information is not available in the RAG knowledge base.
3. **Every answer must include clear citations.**
   - Cite each supporting passage with:
       - file name
       - page number (if provided by metadata)
   Example: *(Source: policy.pdf, page no: 12)*

4. **You must differentiate between retrieved text and your own words.**
   - Use this format:
        - **Retrieved:** quoted passage
        - **Explanation:** your own summary or reasoning

5. **Be concise, accurate, and strictly grounded in retrieved content.**
   - If retrieved content is contradictory, report the conflict.
   - Never fabricate sources or pages.

6. **Do not answer unless at least one passage was retrieved.**
   - If no passages retrieved:
     No relevant content found in the RAG knowledge base.

### Output Format
- Begin with: **Retrieved Sources:** followed by a list of file names + pages.
- Provide the answer, clearly distinguishing retrieved quotes and your explanation.
- End with a summary only if supported by citations.

Your purpose is to provide reliable, citation-bound answers extracted solely from the vector database.
  "
)

# Create chat object with LLM
chat_obj <- ellmer::chat_openai(
  system_prompt,
  base_url = Sys.getenv("OPENAI_BASE_URL"),
  api_key = Sys.getenv("OPENAI_API_KEY"),
  model = OPENAI_CHAT_MODEL
)

# Register the RAG retrieval tool so the LLM can search the vector store
ragnar_register_tool_retrieve(
  chat_obj,
  store_location,
  embed = embed_function,
  top_k = 2L,
  deoverlap = FALSE
)


# --- Example queries
chat_obj$chat("Ask your question here")

