from __future__ import annotations

import re
from typing import Any


def handle(args: dict[str, Any], runtime: Any, host: Any) -> dict[str, Any]:
  payload = args or {}
  raw_url = str(payload.get("url") or "").strip()
  if not raw_url:
    raise ValueError("url is required")
  host.ensure_network_allowed()
  try:
    max_chars = int(payload.get("max_chars") or 6000)
  except (TypeError, ValueError):
    max_chars = 6000
  try:
    max_links = int(payload.get("max_links") or 20)
  except (TypeError, ValueError):
    max_links = 20
  safe_max_chars = max(400, min(40_000, max_chars))
  safe_max_links = max(0, min(100, max_links))

  web_payload = host.fetch_web_url(raw_url)
  content_type = str(web_payload.get("content_type") or "").lower()
  raw_text = str(web_payload.get("text") or "")
  is_html = "html" in content_type

  title = host.extract_html_title(raw_text) if is_html else ""
  content = host.html_to_text(raw_text) if is_html else re.sub(r"\s+", " ", raw_text).strip()
  content = content[:safe_max_chars].strip()
  links = host.extract_html_links(
    raw_text,
    str(web_payload.get("url") or raw_url),
    limit=safe_max_links,
  ) if is_html else []

  return {
    "requested_url": host.normalize_http_url(raw_url),
    "url": str(web_payload.get("url") or raw_url),
    "status_code": int(web_payload.get("status_code") or 200),
    "content_type": str(web_payload.get("content_type") or ""),
    "title": title,
    "content": content,
    "links": links,
    "truncated": bool(web_payload.get("truncated")) or (len(content) >= safe_max_chars),
  }
