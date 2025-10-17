import os
from supabase import create_client, Client
from typing import List, Dict, Any
from langchain.schema.document import Document

# --- 1. Client Initialisierung ---
def get_supabase_client(config: Dict[str, Any]) -> Client | None:
    """Initialisiert den Supabase-Client."""
    supabase_url = config.get("SUPABASE_URL")
    supabase_key = config.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        return None
        
    try:
        client = create_client(supabase_url, supabase_key)
        return client
        
    except Exception as e:
        print(f"❌ Fehler bei der Initialisierung des Supabase-Clients: {e}")
        return None

# --- 2. Speicherung der Vektoren und Metadaten ---
def store_in_supabase(
    client: Client, 
    vectors: List[List[float]], 
    documents: List[Document], 
    table_name: str
):
    """Speichert die Vektoren, Texte und Metadaten in der pgvector-Tabelle."""
    if len(vectors) != len(documents):
        print("FEHLER: Die Anzahl der Vektoren und Dokumente stimmt nicht überein.")
        return

    data_to_insert = []
    
    for i, doc in enumerate(documents):
        data_to_insert.append({
            "content": doc.page_content, 
            "embedding": vectors[i], 
            "metadata": doc.metadata 
        })

    print(f"Versuche, {len(data_to_insert)} Vektor-Einträge in '{table_name}' zu speichern...")
    
    try:
        # Führt den Batch-Insert aus
        response = client.table(table_name).insert(data_to_insert).execute()
        
        if response.data is not None and len(response.data) > 0:
            print(f"✅ Ingest erfolgreich abgeschlossen. {len(response.data)} Datensätze erstellt.")
        elif response.error:
             raise Exception(f"Datenbankfehler: {response.error.message}")
        else:
             print("❌ Ingest schlug fehl, keine Daten zurückgegeben.")

    except Exception as e:
        print(f"❌ Schwerwiegender Fehler beim Datenbank-Ingest: {e}")