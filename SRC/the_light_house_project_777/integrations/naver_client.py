from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Dict, List, Optional

import httpx


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class NaverNewsClient:
    """
    Naver news integration for Korea Update content pipeline.
    Uses Naver open API when configured, otherwise returns deterministic mock results.
    """

    CATEGORIES = ("entertainment", "economy", "technology", "sports")
    CATEGORY_QUERY = {
        "entertainment": "Korea entertainment K-pop",
        "economy": "Korea economy market export",
        "technology": "Korea technology AI chip Samsung SK Hynix",
        "sports": "Korea sports Son Heung-min",
    }

    def __init__(self) -> None:
        self.client_id = (
            os.getenv("NAVER_CLIENT_ID", "").strip()
            or os.getenv("NAVER_API_CLIENT_ID", "").strip()
            or os.getenv("NAVER_NEWS_CLIENT_ID", "").strip()
        )
        self.client_secret = (
            os.getenv("NAVER_CLIENT_SECRET", "").strip()
            or os.getenv("NAVER_API_CLIENT_SECRET", "").strip()
            or os.getenv("NAVER_NEWS_CLIENT_SECRET", "").strip()
        )
        self.base_url = os.getenv("NAVER_NEWS_API_BASE_URL", "https://openapi.naver.com/v1/search/news.json").strip()
        self.image_base_url = os.getenv("NAVER_IMAGE_API_BASE_URL", "https://openapi.naver.com/v1/search/image").strip()
        self.timeout = float(os.getenv("NAVER_TIMEOUT_SEC", "10").strip() or "10")
        self.allow_mock_news = os.getenv("NAVER_ALLOW_MOCK_NEWS", "false").strip().lower() == "true"
        self._last_fetch_status: Dict[str, Dict[str, Any]] = {}

    @property
    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def _set_fetch_status(self, category: str, status: str, detail: str = "", count: int = 0) -> None:
        self._last_fetch_status[str(category or "").strip().lower()] = {
            "status": str(status or "").strip(),
            "detail": str(detail or "").strip(),
            "count": int(count or 0),
            "allow_mock_news": bool(self.allow_mock_news),
            "configured": bool(self.configured),
        }

    def get_last_fetch_status(self, category: str) -> Dict[str, Any]:
        return dict(self._last_fetch_status.get(str(category or "").strip().lower(), {}))

    def _strip_html(self, text: str) -> str:
        no_tag = re.sub(r"<[^>]+>", " ", str(text or ""))
        return re.sub(r"\s+", " ", unescape(no_tag)).strip()

    def _parse_pub_date(self, value: Optional[str]) -> str:
        if not value:
            return _now_iso()
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return _now_iso()

    def _extract_thumbnail_from_html(self, html: str) -> str:
        patterns = (
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+property=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        )
        for pattern in patterns:
            found = re.search(pattern, html, flags=re.IGNORECASE)
            if found:
                return str(found.group(1) or "").strip()
        return ""

    def fetch_top_news(self, category: str, limit: int = 10) -> List[Dict[str, Any]]:
        cat = str(category or "").strip().lower()
        if cat not in self.CATEGORIES:
            self._set_fetch_status(cat, "invalid_category", detail="unsupported category", count=0)
            return []
        count = max(1, min(int(limit or 10), 30))

        if not self.configured:
            if self.allow_mock_news:
                rows = self._mock_top_news(cat, count)
                self._set_fetch_status(cat, "mock_fallback_config_missing", detail="NAVER credentials missing", count=len(rows))
                return rows
            self._set_fetch_status(cat, "config_missing", detail="NAVER credentials missing", count=0)
            return []

        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }
        params = {
            "query": self.CATEGORY_QUERY.get(cat, cat),
            "display": count,
            "start": 1,
            "sort": "date",
        }
        try:
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                resp = client.get(self.base_url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
            items = data.get("items") or []
            rows: List[Dict[str, Any]] = []
            for idx, row in enumerate(items[:count], start=1):
                if not isinstance(row, dict):
                    continue
                link = row.get("originallink") or row.get("link") or ""
                title = self._strip_html(str(row.get("title", "")).strip())
                summary = self._strip_html(str(row.get("description", "")).strip())
                rows.append(
                    {
                        "category": cat,
                        "title": title,
                        "summary": summary,
                        "url": str(link).strip(),
                        "thumbnail_url": "",
                        "published_at": self._parse_pub_date(row.get("pubDate")),
                        "rank": idx,
                        "source": "naver",
                    }
                )
            self._set_fetch_status(cat, "ok", detail="naver_api", count=len(rows))
            return rows
        except Exception as exc:
            if self.allow_mock_news:
                rows = self._mock_top_news(cat, count)
                self._set_fetch_status(
                    cat,
                    "mock_fallback_request_failed",
                    detail=f"{type(exc).__name__}: {exc}",
                    count=len(rows),
                )
                return rows
            self._set_fetch_status(cat, "request_failed", detail=f"{type(exc).__name__}: {exc}", count=0)
            return []

    def get_article_content(self, url: str) -> Dict[str, Any]:
        link = str(url or "").strip()
        if not link:
            return {"ok": False, "url": "", "title": "", "content": "", "thumbnail_url": ""}
        try:
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                resp = client.get(link)
                resp.raise_for_status()
                html = resp.text
        except Exception:
            return {"ok": False, "url": link, "title": "", "content": "", "thumbnail_url": ""}

        title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        title = self._strip_html(title_match.group(1) if title_match else "")
        thumbnail_url = self._extract_thumbnail_from_html(html)
        html_body = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
        html_body = re.sub(r"<style[^>]*>.*?</style>", " ", html_body, flags=re.IGNORECASE | re.DOTALL)
        paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html_body, flags=re.IGNORECASE | re.DOTALL)
        extracted = [self._strip_html(p) for p in paragraphs]
        extracted = [line for line in extracted if len(line) >= 40][:12]
        if not extracted:
            plain = self._strip_html(html_body)
            extracted = [plain[:1400]] if plain else []
        content = "\n".join(extracted)
        return {"ok": True, "url": link, "title": title, "content": content, "thumbnail_url": thumbnail_url}

    def get_article_thumbnail(self, url: str) -> Dict[str, Any]:
        link = str(url or "").strip()
        if not link:
            return {"ok": False, "url": "", "thumbnail_url": ""}
        try:
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                resp = client.get(link)
                resp.raise_for_status()
                html = resp.text
        except Exception:
            return {"ok": False, "url": link, "thumbnail_url": ""}
        thumb = self._extract_thumbnail_from_html(html)
        return {"ok": bool(thumb), "url": link, "thumbnail_url": thumb}

    def search_images(self, query: str, limit: int = 3) -> List[Dict[str, Any]]:
        text = str(query or "").strip()
        if not text:
            return []
        count = max(1, min(int(limit or 3), 10))
        if not self.configured:
            return self._mock_image_search(text, count)
        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }
        params = {
            "query": text,
            "display": count,
            "start": 1,
            "sort": "sim",
            "filter": "medium",
        }
        try:
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                resp = client.get(self.image_base_url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
            rows: List[Dict[str, Any]] = []
            for item in data.get("items") or []:
                if not isinstance(item, dict):
                    continue
                rows.append(
                    {
                        "title": self._strip_html(str(item.get("title", "")).strip()),
                        "image_url": str(item.get("link", "")).strip(),
                        "thumbnail_url": str(item.get("thumbnail", "")).strip(),
                        "sizeheight": item.get("sizeheight"),
                        "sizewidth": item.get("sizewidth"),
                        "query": text,
                        "source": "naver_image_search",
                    }
                )
                if len(rows) >= count:
                    break
            return rows
        except Exception:
            return self._mock_image_search(text, count)

    def _mock_top_news(self, category: str, count: int) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        label = category.title()
        for idx in range(1, count + 1):
            published_at = (now - timedelta(minutes=idx * 8)).isoformat()
            rows.append(
                {
                    "category": category,
                    "title": f"{label} headline {idx} from Korea",
                    "summary": f"Key {label.lower()} update {idx} affecting Korea and global audiences.",
                    "url": f"https://news.example.com/{category}/{idx}",
                    "thumbnail_url": f"https://picsum.photos/seed/{category}-{idx}/640/360",
                    "published_at": published_at,
                    "rank": idx,
                    "source": "mock",
                }
            )
        return rows

    def _mock_image_search(self, query: str, count: int) -> List[Dict[str, Any]]:
        slug = re.sub(r"[^a-z0-9]+", "-", str(query or "").strip().lower()).strip("-") or "workconnect"
        rows: List[Dict[str, Any]] = []
        for idx in range(1, count + 1):
            rows.append(
                {
                    "title": f"{query} visual {idx}",
                    "image_url": f"https://picsum.photos/seed/{slug}-{idx}/1080/1920",
                    "thumbnail_url": f"https://picsum.photos/seed/{slug}-{idx}/540/960",
                    "query": query,
                    "source": "mock_image_search",
                }
            )
        return rows
