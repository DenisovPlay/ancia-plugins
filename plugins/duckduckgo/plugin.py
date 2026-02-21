from __future__ import annotations

import html as html_lib
import re
from typing import Any
from urllib import parse as url_parse


def _strip_html_tags(fragment: str) -> str:
  without_tags = re.sub(r"<[^>]+>", " ", str(fragment or ""), flags=re.DOTALL)
  return re.sub(r"\s+", " ", html_lib.unescape(without_tags)).strip()


def _decode_duckduckgo_result_url(url_like: str) -> str:
  raw = html_lib.unescape(str(url_like or "").strip())
  if not raw:
    return ""

  parsed = url_parse.urlparse(raw)
  query = url_parse.parse_qs(parsed.query)
  uddg = query.get("uddg", [])
  if uddg:
    decoded = url_parse.unquote(uddg[0])
    if decoded:
      return decoded

  if raw.startswith("//"):
    return f"https:{raw}"
  if raw.startswith("/"):
    return url_parse.urljoin("https://duckduckgo.com", raw)
  return raw


def _parse_duckduckgo_results(html: str, *, limit: int = 5) -> list[dict[str, str]]:
  results: list[dict[str, str]] = []
  seen: set[str] = set()

  pattern = re.compile(
    r'<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
    flags=re.IGNORECASE | re.DOTALL,
  )
  for match in pattern.finditer(str(html or "")):
    href = _decode_duckduckgo_result_url(match.group(1))
    title = _strip_html_tags(match.group(2))
    if not href or not title:
      continue
    parsed = url_parse.urlparse(href)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
      continue
    normalized = parsed.geturl()
    if normalized in seen:
      continue
    seen.add(normalized)
    results.append(
      {
        "title": title,
        "url": normalized,
      }
    )
    if len(results) >= max(1, limit):
      break

  return results


def handle(args: dict[str, Any], runtime: Any, host: Any) -> dict[str, Any]:
  payload = args or {}
  query = str(payload.get("query") or "").strip()
  if not query:
    raise ValueError("query is required")
  host.ensure_network_allowed()
  try:
    limit = int(payload.get("limit") or 5)
  except (TypeError, ValueError):
    limit = 5
  safe_limit = max(1, min(10, limit))
  search_url = "https://duckduckgo.com/html/?q=" + url_parse.quote_plus(query)
  web_payload = host.fetch_web_url(search_url)
  results = _parse_duckduckgo_results(web_payload.get("text", ""), limit=safe_limit)
  return {
    "query": query,
    "count": len(results),
    "results": results,
    "source": "duckduckgo",
    "search_url": search_url,
    "response_url": web_payload.get("url", search_url),
    "status_code": web_payload.get("status_code", 200),
  }
