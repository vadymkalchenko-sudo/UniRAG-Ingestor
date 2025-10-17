import json
import os
from typing import Dict, Any

def load_config(path: str = "config.json") -> Dict[str, Any]:
    """
    Lädt die Konfiguration aus JSON und ergänzt sie um kritische
    Variablen aus der Umgebung (ENV).
    """
    print(f"Lade Konfiguration von {path}...")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"❌ Fehler: Konfigurationsdatei {path} nicht gefunden. Beende Programm.")
        exit(1)
    except json.JSONDecodeError:
        print(f"❌ Fehler: Konfigurationsdatei {path} ist kein valides JSON. Beende Programm.")
        exit(1)
        
    # Ergänzung durch ENV-Variablen (müssen in der .env-Datei stehen)
    config["SUPABASE_URL"] = os.getenv("SUPABASE_URL")
    config["SUPABASE_KEY"] = os.getenv("SUPABASE_KEY")
    config["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY") 
    config["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY") 
    
    print("✅ Konfiguration erfolgreich geladen und durch ENV-Variablen ergänzt.")
    return config