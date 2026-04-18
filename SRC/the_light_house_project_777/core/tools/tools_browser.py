from duckduckgo_search import DDGS
from typing import List, Dict, Any

# Allowed domain list injected from main.py.
ALLOWED_DOMAINS: List[str] = []

def web_search(query: str, num_results: int = 5) -> List[Dict[str, Any]]:
    """
    Search the web with DuckDuckGo and return the results.
    If ALLOWED_DOMAINS is configured, only results from those domains are returned.
    """
    results = []
    print(f"Searching web for: {query}")
    with DDGS() as ddgs:
        search_results = ddgs.text(query, max_results=num_results * 2)  # Fetch extra results for filtering.
        for r in search_results:
            if len(results) >= num_results:
                break
            # r is a dict like {'title': '...', 'href': '...', 'body': '...'}
            if ALLOWED_DOMAINS:
                if any(allowed in r.get('href', '') for allowed in ALLOWED_DOMAINS):
                    results.append(r)
            else:
                results.append(r)
    print(f"Found {len(results)} results.")
    return results
