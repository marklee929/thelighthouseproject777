from __future__ import annotations

from typing import Any, Dict
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup


class ArticleContentClient:
    """Fetches article HTML and extracts raw text without persistence logic."""

    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout
        self.user_agent = "LighthouseProject777/phase1-article-fetch"

    def fetch(self, article_url: str) -> Dict[str, Any]:
        target_url = str(article_url or "").strip()
        if not target_url:
            return {"ok": False, "error": "empty article url", "requested_url": "", "final_url": "", "canonical_url": "", "html": "", "text": "", "excerpt": ""}
        try:
            with httpx.Client(
                timeout=self.timeout,
                follow_redirects=True,
                headers={"User-Agent": self.user_agent},
            ) as client:
                response = client.get(target_url)
                response.raise_for_status()
            html = response.text
            soup = BeautifulSoup(html, "html.parser")
            canonical_url = self._canonical_url(soup, str(response.url))
            text = self._extract_text(soup)
            excerpt = text[:500].strip()
            return {
                "ok": True,
                "requested_url": target_url,
                "final_url": str(response.url),
                "canonical_url": canonical_url,
                "html": html,
                "text": text,
                "excerpt": excerpt,
                "status_code": int(response.status_code),
            }
        except Exception as exc:
            return {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "requested_url": target_url,
                "final_url": target_url,
                "canonical_url": target_url,
                "html": "",
                "text": "",
                "excerpt": "",
                "status_code": 0,
            }

    def _canonical_url(self, soup: BeautifulSoup, final_url: str) -> str:
        link = soup.find("link", rel="canonical")
        if link and link.get("href"):
            return urljoin(final_url, str(link.get("href")).strip())
        og_url = soup.find("meta", attrs={"property": "og:url"})
        if og_url and og_url.get("content"):
            return urljoin(final_url, str(og_url.get("content")).strip())
        return final_url

    def _extract_text(self, soup: BeautifulSoup) -> str:
        for tag_name in ("script", "style", "noscript"):
            for tag in soup.find_all(tag_name):
                tag.decompose()
        article = soup.find("article")
        blocks = article.find_all("p") if article else soup.find_all("p")
        lines = []
        for block in blocks:
            text = " ".join(block.get_text(" ", strip=True).split())
            if text:
                lines.append(text)
        if not lines:
            body_text = " ".join(soup.get_text(" ", strip=True).split())
            return body_text[:10000]
        return "\n".join(lines[:120])
