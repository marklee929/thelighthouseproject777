from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import httpx

from integrations.naver_client import NaverNewsClient

from .lmdb_store import normalize_source_url


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class NewsCollector:
    KEYWORD_SETS = {
        "foreign_workers_korea": [
            "foreign workers korea",
            "korea jobs for foreigners",
        ],
        "eps_korea": [
            "eps korea",
            "migrant workers korea",
        ],
        "factory_jobs_korea": [
            "factory jobs korea foreigner",
        ],
    }

    def __init__(self, project_root: str, naver: NaverNewsClient) -> None:
        self.project_root = Path(project_root)
        self.naver = naver
        self.raw_dir = self.project_root / "data" / "news_raw"
        self.filtered_dir = self.project_root / "data" / "news_filtered"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.filtered_dir.mkdir(parents=True, exist_ok=True)
        self.max_age_hours = 72

    def list_keyword_sets(self) -> Dict[str, List[str]]:
        return self.KEYWORD_SETS

    def _fetch_naver_query(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        if not self.naver.configured:
            return self._mock_query(query, limit)
        headers = {
            "X-Naver-Client-Id": self.naver.client_id,
            "X-Naver-Client-Secret": self.naver.client_secret,
        }
        params = {"query": query, "display": max(1, min(limit, 20)), "start": 1, "sort": "date"}
        try:
            with httpx.Client(timeout=self.naver.timeout, follow_redirects=True) as client:
                response = client.get(self.naver.base_url, headers=headers, params=params)
                response.raise_for_status()
                payload = response.json()
            rows: List[Dict[str, Any]] = []
            for idx, item in enumerate(payload.get("items") or [], start=1):
                if not isinstance(item, dict):
                    continue
                link = str(item.get("originallink") or item.get("link") or "").strip()
                rows.append(
                    {
                        "source": "naver",
                        "title": self.naver._strip_html(item.get("title", "")),
                        "summary": self.naver._strip_html(item.get("description", "")),
                        "body_excerpt": self.naver._strip_html(item.get("description", "")),
                        "url": link,
                        "published_at": self.naver._parse_pub_date(item.get("pubDate")),
                        "category_hint": "jobs",
                        "rank": idx,
                    }
                )
            return rows
        except Exception:
            return self._mock_query(query, limit)

    def _mock_query(self, query: str, limit: int) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        for idx in range(1, limit + 1):
            rows.append(
                {
                    "source": "mock",
                    "title": f"{query.title()} update {idx}",
                    "summary": f"Latest hiring and policy signal for {query}.",
                    "body_excerpt": f"Mock excerpt for {query} item {idx}.",
                    "url": f"https://example.com/{hashlib.sha1(f'{query}-{idx}'.encode()).hexdigest()[:12]}",
                    "published_at": (now - timedelta(hours=idx)).isoformat(),
                    "category_hint": "jobs",
                    "rank": idx,
                }
            )
        return rows

    def _article_key(self, row: Dict[str, Any]) -> str:
        normalized = normalize_source_url(str(row.get("url", "")).strip())
        stable_url = (
            str(normalized.get("canonical_article_id", "")).strip()
            or str(normalized.get("normalized_url", "")).strip()
            or str(row.get("url", "")).strip()
        ).lower()
        title = " ".join(str(row.get("title", "")).strip().lower().split())
        base = f"{stable_url}|{title}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    def _within_age(self, published_at: str) -> bool:
        try:
            dt = datetime.fromisoformat(str(published_at or ""))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt >= datetime.now(timezone.utc) - timedelta(hours=self.max_age_hours)
        except Exception:
            return False

    def collect(self, source: str, keyword_set: str, limit_per_keyword: int = 6) -> Dict[str, Any]:
        source_key = str(source or "naver").strip().lower()
        keyword_key = str(keyword_set or "foreign_workers_korea").strip().lower()
        keywords = self.KEYWORD_SETS.get(keyword_key) or self.KEYWORD_SETS["foreign_workers_korea"]

        raw_rows: List[Dict[str, Any]] = []
        deduped: List[Dict[str, Any]] = []
        seen = set()
        for keyword in keywords:
            batch = self._fetch_naver_query(keyword, limit=limit_per_keyword)
            for row in batch:
                merged = {
                    **row,
                    "keywords_hit": [keyword],
                    "collector_source": source_key,
                    "collected_at": _now_iso(),
                }
                raw_rows.append(merged)
                key = self._article_key(merged)
                if key in seen:
                    continue
                seen.add(key)
                if not self._within_age(str(merged.get("published_at", ""))):
                    continue
                deduped.append(merged)

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        raw_path = self.raw_dir / f"{stamp}_{keyword_key}.jsonl"
        filtered_path = self.filtered_dir / f"{stamp}_{keyword_key}.jsonl"
        raw_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in raw_rows), encoding="utf-8")
        filtered_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in deduped), encoding="utf-8")
        return {
            "source": source_key,
            "keyword_set": keyword_key,
            "raw_path": str(raw_path),
            "filtered_path": str(filtered_path),
            "raw_items": raw_rows,
            "filtered_items": deduped,
        }
