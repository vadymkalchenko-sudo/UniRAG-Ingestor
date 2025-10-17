import os
import psycopg2 # Neuer Import für direkte DB-Verbindung
from typing import List, Dict, Any
from langchain.schema.document import Document
from psycopg2.extras import execute_values # Für effizienten Batch-Insert

# --- 1. Client/Verbindung Initialisierung ---
def get_db_connection(config: Dict[str, Any]) -> psycopg2.extensions.connection | None:
    """Initialisiert die PostgreSQL-Verbindung unter Verwendung des vollständigen URI."""
    
    # NEU: Wir laden nur den vollen URI
    db_uri = config.get("DB_CONNECTION_URI") 
    
    if not db_uri:
        print("❌ FEHLER: Die Variable 'DB_CONNECTION_URI' fehlt in der Konfiguration (.env).")
        return None
        
    try:
        # Direkter Verbindungsaufbau mit dem vollen URI (funktioniert mit dem Pooler)
        conn = psycopg2.connect(db_uri)
        conn.autocommit = False  # Wir wollen Transaktionen kontrollieren
        print("✅ PostgreSQL-Verbindung (über URI) erfolgreich initialisiert.")
        return conn
        
    except Exception as e:
        print(f"❌ Fehler bei der Initialisierung der PostgreSQL-Verbindung: {e}")
        return None

# --- 2. Speicherung der Vektoren und Metadaten ---
def store_in_supabase(
    conn: psycopg2.extensions.connection, # Erwartet nun die psycopg2-Verbindung
    vectors: List[List[float]], 
    documents: List[Document], 
    table_name: str
):
    """Speichert die Vektoren, Texte und Metadaten über eine direkte psql-Verbindung."""
    if len(vectors) != len(documents):
        print("FEHLER: Die Anzahl der Vektoren und Dokumente stimmt nicht überein.")
        return

    data_to_insert = []
    
    for i, doc in enumerate(documents):
        # Der Vektor muss als String-Repräsentation für den Insert vorbereitet werden
        vector_str = "[" + ",".join(map(str, vectors[i])) + "]"
        
        data_to_insert.append((
            doc.page_content, 
            vector_str, 
            doc.metadata  # psycopg2 sollte JSON/JSONB korrekt verarbeiten
        ))

    print(f"Versuche, {len(data_to_insert)} Vektor-Einträge in '{table_name}' zu speichern...")
    
    # Wir verwenden den Batch-Insert von psycopg2
    insert_query = f"""
        INSERT INTO {table_name} (content, embedding, metadata)
        VALUES %s
    """
    
    try:
        with conn.cursor() as cur:
            # execute_values ist der effiziente Batch-Insert
            execute_values(cur, insert_query, data_to_insert)
        
        # Transaktion committen
        conn.commit()
        print(f"✅ Ingest erfolgreich abgeschlossen. {len(data_to_insert)} Datensätze erstellt.")

    except Exception as e:
        # Bei Fehler Rollback und Ausgabe des Problems
        conn.rollback()
        print(f"❌ Schwerwiegender Fehler beim Datenbank-Ingest: {e}")

    finally:
        # Verbindung schließen, um Ressourcen freizugeben
        conn.close()