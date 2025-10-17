import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any

def fetch_html(url: str) -> str | None:
    """Holt den HTML-Inhalt von einer URL mit Fehlerprüfung."""
    try:
        response = requests.get(url, headers={'User-Agent': 'UniRAGIngestor/1.0'})
        response.raise_for_status()  # Löst Fehler bei 4xx/5xx aus
        # Wir müssen die Kodierung der 'Gesetze im Internet'-Seite setzen
        response.encoding = response.apparent_encoding 
        return response.text
    except requests.RequestException as e:
        print(f"❌ Fehler beim Abrufen der URL {url}: {e}")
        return None

def parse_html_by_strategy(html_content: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Spezifische Parsing-Logik basierend auf der 'parser_strategy' in der Config.
    """
    strategy = config['parser_strategy']
    print(f"Starte Parsing mit Strategie '{strategy}'...")
    
    if strategy == 'gesetze_im_internet_html':
        return _parse_gesetze_im_internet(html_content, config)
    else:
        raise ValueError(f"❌ Unbekannte Parser-Strategie: {strategy}")

def _parse_gesetze_im_internet(html_content: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Implementierung des robusten Parsers für 'Gesetze im Internet'."""
    soup = BeautifulSoup(html_content, 'lxml')
    selectors = config['selectors']
    documents = []

    # Finde alle Paragraphen-Container (div.jn)
    paragraph_divs = soup.select(selectors['paragraph_container'])
    
    for paragraph_div in paragraph_divs:
        title_element = paragraph_div.select_one(selectors['title_selector'])
        content_element = paragraph_div.select_one(selectors['content_selector'])
        
        if title_element and content_element:
            # Metadaten extrahieren
            heading_text = title_element.get_text(strip=True)
            try:
                # Trennung von Paragraphen-Nummer und Titel (z.B. "§ 1 Inhalt des Gesetzes")
                para_num, para_title = heading_text.split(' ', 1)
            except ValueError:
                para_num = heading_text
                para_title = "" 
            
            # Text-Inhalt extrahieren
            full_content = content_element.get_text(separator="\n", strip=True)

            if full_content:
                documents.append({
                    "content": full_content,
                    "metadata": {
                        "source_url": config['target_url'],
                        "source_id": config['source_id'],
                        "paragraph_number": para_num.strip(),
                        "paragraph_title": para_title.strip()
                    }
                })

    return documents

def crawl(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Haupt-Crawling-Funktion."""
    html = fetch_html(config['target_url'])
    if not html:
        return []
    
    # Der Crawler nutzt die konfigurierte Strategie
    return parse_html_by_strategy(html, config)