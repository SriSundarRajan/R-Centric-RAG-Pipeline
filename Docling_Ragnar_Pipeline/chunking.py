
def get_chunks(source):
    from docling.document_converter import DocumentConverter
    from docling.chunking import HybridChunker

    converter = DocumentConverter()
    result = converter.convert(source)
    docs = result.document

    chunker = HybridChunker()
    chunk_iter = chunker.chunk(dl_doc=docs)

    # Convert to list of text
    return [chunk.text for chunk in chunk_iter]

