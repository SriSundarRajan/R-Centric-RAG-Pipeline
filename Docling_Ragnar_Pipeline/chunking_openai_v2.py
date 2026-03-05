def get_chunks_with_tokens(source, max_tokens=8000, model="text-embedding-3-small"):
    """Get chunks with token limit control using Docling's tokenizer"""
    from docling.document_converter import DocumentConverter
    from docling.chunking import HybridChunker
    from docling_core.transforms.chunker.tokenizer.openai import OpenAITokenizer
    import tiktoken

    # Initialize tokenizer
    tokenizer = OpenAITokenizer(
        tokenizer=tiktoken.encoding_for_model(model),
        max_tokens=max_tokens,
    )
    
    # Convert document
    converter = DocumentConverter()
    result = converter.convert(source)
    docs = result.document

    # Initialize chunker with tokenizer
    chunker = HybridChunker(
        tokenizer=tokenizer,
        merge_peers=True,
    )
    
    # Get chunks
    chunk_iter = chunker.chunk(dl_doc=docs)
    chunks_token = [chunk.text for chunk in chunk_iter]
    
    return chunks_token

