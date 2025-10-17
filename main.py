import os
from dotenv import load_dotenv

import config_loader
import crawler
import processor
import database

def setup_environment() -> bool:
    """Lädt Umgebungsvariablen und prüft auf die wichtigsten Schlüssel."""
    print("Starte Setup und lade Umgebungsvariablen (.env)...")
    load_dotenv()
    
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_KEY"):
        print("❌ FEHLER: Supabase-Zugangsdaten (URL/Key) fehlen. Setup unvollständig.")
        return False
    
    return True

def main():
    """Haupt-Ausführungsfunktion des UniRAG-Ingestor-Agenten."""
    print("==================================================")
    print("  🚀 STARTE UniRAG-Ingestor (RAG Pipeline)")
    print("==================================================")
    
    if not setup_environment():
        print("Pipeline-Start fehlgeschlagen. Breche ab.")
        return

    # 1. Konfiguration laden
    try:
        config = config_loader.load_config("config.json")
        db_table_name = config['DB_TABLE_NAME']
    except Exception as e:
        print(f"❌ FEHLER beim Laden/Prüfen der Konfiguration: {e}")
        return

    # 2. Crawlen
    print("\n--- Phase 1: Crawling ---")
    raw_documents = crawler.crawl(config)
    if not raw_documents: return

    # 3. Verarbeiten (Chunking & Embedding)
    print("\n--- Phase 2: Verarbeitung ---")
    try:
        embedding_service = processor.initialize_embeddings(config)
    except ValueError as e:
        print(f"❌ FEHLER bei Embedding-Initialisierung: {e}. Breche ab.")
        return
        
    vectors, documents = processor.process_documents(raw_documents, config, embedding_service)
    if not vectors or not documents: return

    # 4. Speichern (Datenbank-Ingest)
    print("\n--- Phase 3: Speicherung ---")
    supabase_client = database.get_supabase_client(config)
    if not supabase_client:
        print("Speicherung fehlgeschlagen: Supabase-Client nicht verfügbar. Breche ab.")
        return
        
    database.store_in_supabase(supabase_client, vectors, documents, db_table_name)

    print("\n==================================================")
    print("✅ UniRAG-Ingestor-Lauf erfolgreich abgeschlossen.")
    print("==================================================")

if __name__ == "__main__":
    main()