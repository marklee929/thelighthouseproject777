from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup


def _local_name(tag: str) -> str:
    return str(tag or "").rsplit("}", 1)[-1].lower()


def _child_text(node: ET.Element, *names: str) -> str:
    targets = {name.lower() for name in names}
    for child in list(node):
        if _local_name(child.tag) in targets:
            return (child.text or "").strip()
    return ""


def _entry_link(node: ET.Element, feed_url: str) -> str:
    for child in list(node):
        if _local_name(child.tag) != "link":
            continue
        href = str(child.attrib.get("href") or child.text or "").strip()
        rel = str(child.attrib.get("rel", "alternate")).strip().lower()
        if href and rel in {"alternate", ""}:
            return urljoin(feed_url, href)
    return ""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class RssFeedClient:
    """Fetches RSS, Atom, and RDF feeds without embedding business rules."""

    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout
        self.user_agent = "LighthouseProject777/phase1-rss-ingestion"

    def fetch(self, feed_definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        feed_url = str(feed_definition.get("feed_url", "")).strip()
        with httpx.Client(
            timeout=self.timeout,
            follow_redirects=True,
            headers={"User-Agent": self.user_agent},
        ) as client:
            response = client.get(feed_url)
            response.raise_for_status()
            response = self._resolve_feed_response(client, response)
        active_feed_url = str(response.url)
        root = ET.fromstring(response.content)
        root_name = _local_name(root.tag)
        if root_name == "rss":
            return self._parse_rss(root, active_feed_url)
        if root_name == "feed":
            return self._parse_atom(root, active_feed_url)
        if root_name == "rdf":
            return self._parse_rdf(root, active_feed_url)
        raise ValueError(f"unsupported feed format: {root.tag}")

    def _parse_rss(self, root: ET.Element, feed_url: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        channel = next((child for child in list(root) if _local_name(child.tag) == "channel"), None)
        if channel is None:
            return items
        feed_title = _child_text(channel, "title")
        for item in list(channel):
            if _local_name(item.tag) != "item":
                continue
            items.append(
                {
                    "feed_title": feed_title,
                    "external_id": _child_text(item, "guid") or _child_text(item, "id"),
                    "title": _child_text(item, "title"),
                    "link": _child_text(item, "link"),
                    "summary": _child_text(item, "description", "summary"),
                    "author": _child_text(item, "author", "creator"),
                    "published_at": _child_text(item, "pubdate", "published", "updated"),
                    "raw_collected_at": _utcnow(),
                    "feed_url": feed_url,
                }
            )
        return items

    def _parse_atom(self, root: ET.Element, feed_url: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        feed_title = _child_text(root, "title")
        for entry in list(root):
            if _local_name(entry.tag) != "entry":
                continue
            author = ""
            for child in list(entry):
                if _local_name(child.tag) == "author":
                    author = _child_text(child, "name")
                    break
            items.append(
                {
                    "feed_title": feed_title,
                    "external_id": _child_text(entry, "id"),
                    "title": _child_text(entry, "title"),
                    "link": _entry_link(entry, feed_url),
                    "summary": _child_text(entry, "summary", "content"),
                    "author": author,
                    "published_at": _child_text(entry, "published", "updated"),
                    "raw_collected_at": _utcnow(),
                    "feed_url": feed_url,
                }
            )
        return items

    def _parse_rdf(self, root: ET.Element, feed_url: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        channel = next((child for child in list(root) if _local_name(child.tag) == "channel"), None)
        feed_title = _child_text(channel, "title") if channel is not None else ""
        for item in list(root):
            if _local_name(item.tag) != "item":
                continue
            items.append(
                {
                    "feed_title": feed_title,
                    "external_id": _child_text(item, "guid") or _child_text(item, "identifier"),
                    "title": _child_text(item, "title"),
                    "link": _child_text(item, "link"),
                    "summary": _child_text(item, "description"),
                    "author": _child_text(item, "creator"),
                    "published_at": _child_text(item, "date", "pubdate"),
                    "raw_collected_at": _utcnow(),
                    "feed_url": feed_url,
                }
            )
        return items

    def _resolve_feed_response(self, client: httpx.Client, response: httpx.Response) -> httpx.Response:
        content_type = str(response.headers.get("content-type", "")).lower()
        if "xml" in content_type or "rss" in content_type or "atom" in content_type:
            return response
        discovered_url = self._discover_feed_url(response.text, str(response.url))
        if not discovered_url:
            return response
        discovered_response = client.get(discovered_url)
        discovered_response.raise_for_status()
        return discovered_response

    def _discover_feed_url(self, html: str, base_url: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        candidates: List[str] = []
        for link in soup.find_all("link"):
            rel = " ".join(link.get("rel") or [])
            href = str(link.get("href") or "").strip()
            feed_type = str(link.get("type") or "").lower()
            if href and "alternate" in rel and ("rss" in feed_type or "atom" in feed_type or "xml" in feed_type):
                candidates.append(urljoin(base_url, href))
        for anchor in soup.find_all("a", href=True):
            href = str(anchor["href"]).strip()
            if not href:
                continue
            lowered = href.lower()
            if lowered == "/rss" or lowered.endswith("/rss") or lowered.endswith(".xml") or lowered.endswith(".rdf") or "/feed" in lowered:
                candidates.append(urljoin(base_url, href))
        seen = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            return candidate
        return ""
