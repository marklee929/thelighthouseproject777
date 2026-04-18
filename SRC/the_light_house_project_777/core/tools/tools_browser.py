from duckduckgo_search import DDGS
from typing import List, Dict, Any

# 허용된 도메인 목록 (main.py에서 주입)
ALLOWED_DOMAINS: List[str] = []

def web_search(query: str, num_results: int = 5) -> List[Dict[str, Any]]:
    """
    DuckDuckGo를 사용하여 웹을 검색하고 결과를 반환합니다.
    ALLOWED_DOMAINS가 설정된 경우, 해당 도메인의 결과만 필터링합니다.
    """
    results = []
    print(f"Searching web for: {query}")
    with DDGS() as ddgs:
        search_results = ddgs.text(query, max_results=num_results * 2) # 필터링을 위해 더 많이 가져옴
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