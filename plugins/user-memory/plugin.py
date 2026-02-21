from __future__ import annotations

import datetime as dt
import re
import uuid
from typing import Any

STORAGE_KEY = "plugin.user-memory.entries.v1"
MAX_ENTRIES = 400
MAX_FACT_LEN = 1200
MAX_KEY_LEN = 72
MAX_TAGS = 10
MAX_TAG_LEN = 32

# Локальные группы синонимов для кросс-языкового поиска (RU/EN) по ключам и тегам.
_QUERY_SYNONYM_GROUPS: list[set[str]] = [
  {
    "phone", "smartphone", "mobile", "cellphone", "iphone", "android",
    "телефон", "смартфон", "мобильник", "сотовый", "айфон", "андроид",
  },
  {
    "device", "gadget", "hardware",
    "устройство", "девайс", "гаджет",
  },
  {
    "laptop", "notebook", "macbook",
    "ноутбук", "лэптоп", "макбук",
  },
]


def _now_utc_iso() -> str:
  return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _normalize_text(value: Any, *, max_len: int = 0) -> str:
  text = re.sub(r"\s+", " ", str(value or "")).strip()
  if max_len > 0 and len(text) > max_len:
    text = text[:max_len].rstrip()
  return text


def _normalize_token(value: Any, *, max_len: int) -> str:
  text = _normalize_text(value, max_len=max(24, max_len * 3)).lower()
  if not text:
    return ""
  text = re.sub(r"\s+", "-", text)
  text = re.sub(r"[^\w.\-]+", "", text, flags=re.UNICODE)
  text = text.strip("._-")
  if len(text) > max_len:
    text = text[:max_len].rstrip("._-")
  return text


def _normalize_tags(value: Any) -> list[str]:
  if not isinstance(value, list):
    return []
  tags: list[str] = []
  seen: set[str] = set()
  for raw in value[: MAX_TAGS * 4]:
    tag = _normalize_token(raw, max_len=MAX_TAG_LEN)
    if not tag or tag in seen:
      continue
    seen.add(tag)
    tags.append(tag)
    if len(tags) >= MAX_TAGS:
      break
  return tags


def _merge_tags(left: list[str], right: list[str]) -> list[str]:
  merged: list[str] = []
  seen: set[str] = set()
  for source in [left, right]:
    for tag in source:
      safe_tag = _normalize_token(tag, max_len=MAX_TAG_LEN)
      if not safe_tag or safe_tag in seen:
        continue
      seen.add(safe_tag)
      merged.append(safe_tag)
      if len(merged) >= MAX_TAGS:
        return merged
  return merged


def _safe_int(value: Any, *, fallback: int, min_value: int, max_value: int) -> int:
  try:
    parsed = int(value)
  except (TypeError, ValueError):
    return fallback
  return max(min_value, min(max_value, parsed))


def _tokenize_query(value: str) -> list[str]:
  safe = _normalize_text(value, max_len=220).lower()
  if not safe:
    return []
  tokens = re.findall(r"[^\W_]{2,}", safe, flags=re.UNICODE)
  deduped: list[str] = []
  seen: set[str] = set()
  for token in tokens:
    if token in seen:
      continue
    seen.add(token)
    deduped.append(token)
    if len(deduped) >= 12:
      break
  return deduped


def _normalize_term(value: Any) -> str:
  token = _normalize_token(value, max_len=48)
  if token:
    return token
  return _normalize_text(value, max_len=48).lower()


def _build_synonym_aliases() -> dict[str, set[str]]:
  aliases: dict[str, set[str]] = {}
  for group in _QUERY_SYNONYM_GROUPS:
    normalized_group: set[str] = set()
    for item in group:
      safe_item = _normalize_term(item)
      if safe_item:
        normalized_group.add(safe_item)
    for item in normalized_group:
      aliases[item] = set(normalized_group)
  return aliases


_QUERY_SYNONYM_ALIASES = _build_synonym_aliases()


def _expand_query_terms(terms: list[str]) -> list[str]:
  expanded: list[str] = []
  seen: set[str] = set()
  for raw in terms:
    term = _normalize_term(raw)
    if not term:
      continue
    if term not in seen:
      seen.add(term)
      expanded.append(term)
    for synonym in sorted(_QUERY_SYNONYM_ALIASES.get(term, set())):
      if synonym in seen:
        continue
      seen.add(synonym)
      expanded.append(synonym)
  return expanded


def _parse_iso(value: Any) -> dt.datetime:
  raw = _normalize_text(value, max_len=64)
  if not raw:
    return dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
  try:
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    parsed = dt.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
      parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)
  except ValueError:
    return dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)


def _new_memory_id() -> str:
  return f"mem-{uuid.uuid4().hex[:12]}"


def _normalize_memory_entry(raw: Any) -> dict[str, Any] | None:
  if not isinstance(raw, dict):
    return None
  fact = _normalize_text(raw.get("fact"), max_len=MAX_FACT_LEN)
  if not fact:
    return None
  entry_id = _normalize_text(raw.get("id"), max_len=120) or _new_memory_id()
  key = _normalize_token(raw.get("key"), max_len=MAX_KEY_LEN)
  tags = _normalize_tags(raw.get("tags"))
  importance = _safe_int(raw.get("importance"), fallback=3, min_value=1, max_value=5)
  created_at = _normalize_text(raw.get("created_at"), max_len=64) or _now_utc_iso()
  updated_at = _normalize_text(raw.get("updated_at"), max_len=64) or created_at
  user_name = _normalize_text(raw.get("user_name"), max_len=96)
  chat_id = _normalize_text(raw.get("chat_id"), max_len=96)
  return {
    "id": entry_id,
    "key": key,
    "fact": fact,
    "tags": tags,
    "importance": importance,
    "created_at": created_at,
    "updated_at": updated_at,
    "user_name": user_name,
    "chat_id": chat_id,
  }


def _load_entries(host: Any) -> list[dict[str, Any]]:
  raw = host.storage.get_setting_json(STORAGE_KEY, [])
  if not isinstance(raw, list):
    return []
  entries: list[dict[str, Any]] = []
  seen_ids: set[str] = set()
  for item in raw:
    normalized = _normalize_memory_entry(item)
    if not normalized:
      continue
    memory_id = str(normalized.get("id") or "")
    if memory_id in seen_ids:
      normalized["id"] = _new_memory_id()
      memory_id = normalized["id"]
    seen_ids.add(memory_id)
    entries.append(normalized)
  return entries


def _entry_sort_key(entry: dict[str, Any]) -> tuple[float, int]:
  return (_parse_iso(entry.get("updated_at")).timestamp(), int(entry.get("importance") or 3))


def _save_entries(host: Any, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
  cleaned: list[dict[str, Any]] = []
  seen_ids: set[str] = set()
  for item in sorted(entries, key=_entry_sort_key, reverse=True):
    normalized = _normalize_memory_entry(item)
    if not normalized:
      continue
    memory_id = str(normalized.get("id") or "")
    if not memory_id or memory_id in seen_ids:
      normalized["id"] = _new_memory_id()
      memory_id = normalized["id"]
    seen_ids.add(memory_id)
    cleaned.append(normalized)
    if len(cleaned) >= MAX_ENTRIES:
      break
  host.storage.set_setting_json(STORAGE_KEY, cleaned)
  return cleaned


def _resolve_scope(value: Any) -> str:
  scope = _normalize_text(value, max_len=24).lower()
  return scope if scope in {"current_user", "all"} else "current_user"


def _matches_scope(entry: dict[str, Any], *, scope: str, runtime_user_name: str) -> bool:
  if scope == "all":
    return True
  safe_runtime_user = _normalize_text(runtime_user_name, max_len=96).lower()
  if not safe_runtime_user:
    return True
  entry_user = _normalize_text(entry.get("user_name"), max_len=96).lower()
  # Include user-specific entries and global entries (empty user_name).
  return entry_user in {"", safe_runtime_user}


def _public_memory(entry: dict[str, Any], *, include_user: bool) -> dict[str, Any]:
  payload: dict[str, Any] = {
    "id": str(entry.get("id") or ""),
    "key": str(entry.get("key") or ""),
    "fact": str(entry.get("fact") or ""),
    "tags": list(entry.get("tags") or []),
    "importance": int(entry.get("importance") or 3),
    "updated_at": str(entry.get("updated_at") or ""),
  }
  if include_user:
    payload["user_name"] = str(entry.get("user_name") or "")
  return payload


def remember(args: dict[str, Any], runtime: Any, host: Any) -> dict[str, Any]:
  payload = args or {}
  fact = _normalize_text(payload.get("fact"), max_len=MAX_FACT_LEN)
  if not fact:
    raise ValueError("fact is required")

  key = _normalize_token(payload.get("key"), max_len=MAX_KEY_LEN)
  tags = _normalize_tags(payload.get("tags"))
  importance = _safe_int(payload.get("importance"), fallback=3, min_value=1, max_value=5)
  overwrite_key = bool(payload.get("overwrite_key", True))

  runtime_user_name = _normalize_text(getattr(runtime, "user_name", ""), max_len=96)
  runtime_chat_id = _normalize_text(getattr(runtime, "chat_id", ""), max_len=96)
  now = _now_utc_iso()

  entries = _load_entries(host)
  target_index = -1

  fact_lc = fact.lower()
  for index, entry in enumerate(entries):
    if not _matches_scope(entry, scope="current_user", runtime_user_name=runtime_user_name):
      continue
    if str(entry.get("fact") or "").strip().lower() == fact_lc:
      target_index = index
      break

  if target_index < 0 and key and overwrite_key:
    for index, entry in enumerate(entries):
      if not _matches_scope(entry, scope="current_user", runtime_user_name=runtime_user_name):
        continue
      if _normalize_token(entry.get("key"), max_len=MAX_KEY_LEN) == key:
        target_index = index
        break

  action = "saved"
  if target_index >= 0:
    existing = dict(entries[target_index])
    existing_tags = _normalize_tags(existing.get("tags"))
    merged_tags = _merge_tags(existing_tags, tags)
    existing["fact"] = fact
    existing["key"] = key or _normalize_token(existing.get("key"), max_len=MAX_KEY_LEN)
    existing["tags"] = merged_tags
    existing["importance"] = importance
    existing["updated_at"] = now
    existing["chat_id"] = runtime_chat_id or _normalize_text(existing.get("chat_id"), max_len=96)
    existing["user_name"] = runtime_user_name or _normalize_text(existing.get("user_name"), max_len=96)
    if not _normalize_text(existing.get("created_at"), max_len=64):
      existing["created_at"] = now
    entries[target_index] = existing
    action = "updated"
    saved_entry = existing
  else:
    saved_entry = {
      "id": _new_memory_id(),
      "key": key,
      "fact": fact,
      "tags": tags,
      "importance": importance,
      "created_at": now,
      "updated_at": now,
      "chat_id": runtime_chat_id,
      "user_name": runtime_user_name,
    }
    entries.append(saved_entry)

  entries = _save_entries(host, entries)
  include_user = False
  return {
    "status": action,
    "memory": _public_memory(saved_entry, include_user=include_user),
    "total_memories": len(entries),
    "request_id": host.create_request_id(),
  }


def recall(args: dict[str, Any], runtime: Any, host: Any) -> dict[str, Any]:
  payload = args or {}
  query = _normalize_text(payload.get("query"), max_len=200)
  key = _normalize_token(payload.get("key"), max_len=MAX_KEY_LEN)
  tags = _normalize_tags(payload.get("tags"))
  scope = _resolve_scope(payload.get("scope"))
  limit = _safe_int(payload.get("limit"), fallback=5, min_value=1, max_value=20)
  query_terms = _expand_query_terms(_tokenize_query(query))

  runtime_user_name = _normalize_text(getattr(runtime, "user_name", ""), max_len=96)
  include_user = scope == "all"
  entries = _load_entries(host)

  ranked: list[tuple[float, float, dict[str, Any]]] = []
  for entry in entries:
    if not _matches_scope(entry, scope=scope, runtime_user_name=runtime_user_name):
      continue

    entry_key = _normalize_token(entry.get("key"), max_len=MAX_KEY_LEN)
    entry_tags = _normalize_tags(entry.get("tags"))
    if key and entry_key != key:
      continue
    if tags and not set(tags).issubset(set(entry_tags)):
      continue

    score = float(int(entry.get("importance") or 3))
    searchable_fact = str(entry.get("fact") or "").lower()
    searchable_key = entry_key.lower()
    searchable_tags = [str(tag).lower() for tag in entry_tags]
    entry_terms = _expand_query_terms(
      _tokenize_query(
        " ".join([
          searchable_fact,
          searchable_key,
          " ".join(searchable_tags),
        ])
      )
    )
    entry_term_set = set(entry_terms)
    if query_terms:
      hits = 0
      for term in query_terms:
        if term in searchable_key:
          score += 18
          hits += 1
        if term in searchable_fact:
          score += 12
          hits += 1
        if any(term in tag for tag in searchable_tags):
          score += 10
          hits += 1
        if term in entry_term_set:
          score += 8
          hits += 1
      if hits == 0:
        continue

    if key and entry_key == key:
      score += 80
    if tags:
      score += 10

    ranked.append((score, _parse_iso(entry.get("updated_at")).timestamp(), entry))

  if query_terms or key or tags:
    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
  else:
    ranked.sort(key=lambda item: (item[1], item[0]), reverse=True)

  selected = [item[2] for item in ranked[:limit]]
  memories = [_public_memory(entry, include_user=include_user) for entry in selected]
  results = [
    {
      "title": memory.get("fact", ""),
      "snippet": ", ".join(
        part for part in [
          f"key={memory.get('key')}" if memory.get("key") else "",
          f"tags={','.join(memory.get('tags') or [])}" if memory.get("tags") else "",
          f"importance={memory.get('importance')}",
          f"user={memory.get('user_name')}" if include_user and memory.get("user_name") else "",
          f"updated_at={memory.get('updated_at')}" if memory.get("updated_at") else "",
        ] if part
      ),
    }
    for memory in memories
  ]

  return {
    "query": query,
    "key": key,
    "tags": tags,
    "scope": scope,
    "count": len(memories),
    "memories": memories,
    "results": results,
    "request_id": host.create_request_id(),
  }


def forget(args: dict[str, Any], runtime: Any, host: Any) -> dict[str, Any]:
  payload = args or {}
  target_id = _normalize_text(payload.get("id"), max_len=120)
  key = _normalize_token(payload.get("key"), max_len=MAX_KEY_LEN)
  query = _normalize_text(payload.get("query"), max_len=200)
  scope = _resolve_scope(payload.get("scope"))
  all_matching = bool(payload.get("all_matching", False))
  query_terms = _tokenize_query(query)

  if not target_id and not key and not query_terms:
    raise ValueError("at least one of id, key, or query is required")

  runtime_user_name = _normalize_text(getattr(runtime, "user_name", ""), max_len=96)
  entries = _load_entries(host)
  kept: list[dict[str, Any]] = []
  removed: list[dict[str, Any]] = []
  include_user = scope == "all"

  for entry in entries:
    if not _matches_scope(entry, scope=scope, runtime_user_name=runtime_user_name):
      kept.append(entry)
      continue

    entry_id = _normalize_text(entry.get("id"), max_len=120)
    entry_key = _normalize_token(entry.get("key"), max_len=MAX_KEY_LEN)
    search_blob = " ".join([
      str(entry.get("fact") or ""),
      entry_key,
      " ".join(_normalize_tags(entry.get("tags"))),
    ]).lower()

    matches = False
    if target_id and entry_id == target_id:
      matches = True
    if key and entry_key == key:
      matches = True
    if query_terms and all(term in search_blob for term in query_terms):
      matches = True

    if matches and (all_matching or not removed):
      removed.append(entry)
    else:
      kept.append(entry)

  if removed:
    kept = _save_entries(host, kept)

  return {
    "removed_count": len(removed),
    "removed": [_public_memory(entry, include_user=include_user) for entry in removed[:20]],
    "remaining_count": len(kept) if removed else len(entries),
    "scope": scope,
    "request_id": host.create_request_id(),
  }
