import os
from typing import List, Dict, Any, Tuple
from langchain.schema.document import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_google_genai import GoogleGenerativeAIEmbeddings # Für Google-Modelle

# --- 1. Embedding-Initialisierung ---
def initialize_embeddings(config: Dict[str, Any]):
    """Initialisiert den konfigurierten Embedding-Dienst über ENV-Variablen."""
    service_type = config.get("EMBEDDING_SERVICE_TYPE", "OPENAI").upper()
    
    if service_type == "OPENAI":
        model = config.get("EMBEDDING_MODEL_OPENAI")
        if not os.getenv("OPENAI_API_KEY"):
             raise ValueError("OPENAI_API_KEY fehlt in der Umgebung.")
        print(f"Verwende OpenAI Embeddings ({model}).")
        return OpenAIEmbeddings(model=model)
    
    elif service_type == "GOOGLE":
        model = config.get("EMBEDDING_MODEL_GOOGLE")
        if not os.getenv("GOOGLE_API_KEY"):
             raise ValueError("GOOGLE_API_KEY fehlt in der Umgebung.")
        print(f"Verwende Google Embeddings ({model}).")
        return GoogleGenerativeAIEmbeddings(model=model)
        
    else:
        raise ValueError(f"Unbekannter Embedding-Dienst: {service_type}")

# --- 2. Verarbeitung und Chunking ---
def process_documents(raw_docs: List[Dict[str, Any]], config: Dict[str, Any], embedding_service) -> Tuple[List[List[float]], List[Document]]:
    """
    Verarbeitet die Roh-Dokumente, führt Chunking durch und erzeugt Embeddings.
    """
    
    # 1. In LangChain Documents umwandeln (primäre semantische Einheit = Paragraph)
    langchain_docs = [
        Document(page_content=doc['content'], metadata=doc['metadata']) 
        for doc in raw_docs
    ]

    # 2. Fallback-Chunking mit konfigurierbarem Separator
    chunk_config = config['chunking_strategy']
    
    # Der konfigurierte Separator (z.B. "§") wird als erster Split-Punkt verwendet (Musterwerkzeug-Fähigkeit)
    custom_separators = [
        chunk_config.get('semantischer_separator', "\n\n"), # Fallback auf Absätze
        "\n\n", "\n", ". ", " ", ""
    ]
    
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_config['fallback_chunk_size'],
        chunk_overlap=chunk_config['fallback_chunk_overlap'],
        separators=custom_separators
    )
    
    split_docs = text_splitter.split_documents(langchain_docs)
    print(f"Verarbeite {len(raw_docs)} Paragraphen, resultiert in {len(split_docs)} Chunks.")

    # 3. Embeddings erzeugen
    texts_to_embed = [doc.page_content for doc in split_docs]
    
    try:
        vectors = embedding_service.embed_documents(texts_to_embed)
        print("✅ Embeddings erfolgreich erzeugt.")
        return vectors, split_docs
    except Exception as e:
        print(f"❌ Fehler bei der Vektorisierung: {e}")
        return [], []