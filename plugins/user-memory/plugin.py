from __future__ import annotations

import datetime as dt
import json
import math
import re
import sqlite3
import uuid
from difflib import SequenceMatcher
from typing import Any

STORAGE_KEY = "plugin.user-memory.entries.v1"
SQLITE_MIGRATION_FLAG_KEY = "plugin.user-memory.sqlite_migrated.v2"

SQL_TABLE = "plugin_user_memory_entries"
SQL_FTS_TABLE = "plugin_user_memory_entries_fts"

MAX_ENTRIES = 2000
JSON_MIRROR_MAX = 600
MAX_FACT_LEN = 1200
MAX_KEY_LEN = 72
MAX_TAGS = 12
MAX_TAG_LEN = 32
MAX_VECTOR_TERMS = 220

_SQLITE_SCHEMA_READY = False
_SQLITE_FTS_ENABLED = False
_SQLITE_UNAVAILABLE = False

# Cross-lingual synonym groups used for hybrid recall.
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
    "name", "fullname", "nickname",
    "имя", "фио", "ник", "никнейм",
  },
  {
    "city", "town", "location",
    "город", "локация", "место",
  },
  {
    "profession", "job", "role", "developer", "engineer", "programmer",
    "профессия", "работа", "роль", "разработчик", "инженер", "программист",
  },
  {
    "email", "mail",
    "почта", "емейл", "email",
  },
  {
    "timezone", "time-zone",
    "часовой", "пояс", "таймзона",
  },
]

_KEY_ALIAS_RAW: dict[str, str] = {
  "телефон": "phone",
  "смартфон": "phone",
  "iphone": "phone",
  "android": "phone",
  "mobile": "phone",
  "cellphone": "phone",
  "name": "name",
  "имя": "name",
  "nickname": "name",
  "city": "city",
  "город": "city",
  "location": "city",
  "profession": "profession",
  "job": "profession",
  "developer": "profession",
  "engineer": "profession",
  "programmer": "profession",
  "программист": "profession",
  "профессия": "profession",
  "email": "email",
  "mail": "email",
  "почта": "email",
  "timezone": "timezone",
  "таймзона": "timezone",
  "device": "device",
  "устройство": "device",
}

_DEFAULT_TAGS_BY_KEY: dict[str, list[str]] = {
  "phone": ["device", "phone"],
  "device": ["device"],
  "name": ["profile"],
  "city": ["location"],
  "profession": ["profile", "work"],
  "email": ["contact"],
  "timezone": ["profile", "timezone"],
}

_QUERY_STOPWORDS: set[str] = {
  "какой",
  "какая",
  "какое",
  "какие",
  "кто",
  "что",
  "где",
  "когда",
  "почему",
  "зачем",
  "как",
  "мне",
  "меня",
  "мой",
  "моя",
  "мое",
  "мои",
  "у",
  "про",
  "обо",
  "об",
  "about",
  "what",
  "which",
  "who",
  "where",
  "when",
  "why",
  "how",
  "my",
  "me",
  "i",
  "you",
}

_GENERIC_RECALL_PATTERNS: tuple[str, ...] = (
  r"\bчто\s+ты\s+(обо\s+мне\s+)?помни\w*\b",
  r"\bчто\s+ты\s+знаешь\s+обо\s+мне\b",
  r"\bнапомни(?:\s+мне)?\b",
  r"\bwhat\s+do\s+you\s+remember(?:\s+about\s+me)?\b",
  r"\bwhat\s+do\s+you\s+know\s+about\s+me\b",
)

_SLOT_HINT_RULES: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = [
  (
    "phone",
    (
      r"\biphone\b",
      r"\bandroid\b",
      r"\bphone\b",
      r"\bsmartphone\b",
      r"\bтелефон\b",
      r"\bсмартфон\b",
      r"\bайфон\b",
    ),
    ("device", "phone"),
  ),
  (
    "name",
    (
      r"\bmy name is\b",
      r"\bi am\b",
      r"\bменя зовут\b",
      r"\bмо[её] имя\b",
      r"\bзовут\b",
    ),
    ("profile",),
  ),
  (
    "city",
    (
      r"\bi live in\b",
      r"\bfrom\b",
      r"\bживу\b",
      r"\bмой город\b",
      r"\bгород\b",
    ),
    ("location",),
  ),
  (
    "profession",
    (
      r"\bработаю\b",
      r"\bпрофессия\b",
      r"\bпрограммист\b",
      r"\bdeveloper\b",
      r"\bengineer\b",
      r"\bprogrammer\b",
      r"\bmy job\b",
    ),
    ("profile", "work"),
  ),
  (
    "email",
    (
      r"@",
      r"\bemail\b",
      r"\be-mail\b",
      r"\bпочта\b",
      r"\bемейл\b",
    ),
    ("contact",),
  ),
  (
    "timezone",
    (
      r"\btimezone\b",
      r"\btime zone\b",
      r"\bчасов(ой|ого)\b",
      r"\bпояс\b",
      r"\bтаймзон\b",
    ),
    ("profile", "timezone"),
  ),
]

_CYRILLIC_TO_LATIN: dict[str, str] = {
  "а": "a",
  "б": "b",
  "в": "v",
  "г": "g",
  "д": "d",
  "е": "e",
  "ё": "e",
  "ж": "zh",
  "з": "z",
  "и": "i",
  "й": "y",
  "к": "k",
  "л": "l",
  "м": "m",
  "н": "n",
  "о": "o",
  "п": "p",
  "р": "r",
  "с": "s",
  "т": "t",
  "у": "u",
  "ф": "f",
  "х": "h",
  "ц": "ts",
  "ч": "ch",
  "ш": "sh",
  "щ": "shch",
  "ъ": "",
  "ы": "y",
  "ь": "",
  "э": "e",
  "ю": "yu",
  "я": "ya",
}


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
_KEY_ALIAS_MAP = {
  _normalize_term(raw_key): _normalize_term(raw_value)
  for raw_key, raw_value in _KEY_ALIAS_RAW.items()
}
_KNOWN_INFER_KEYS: set[str] = {
  _normalize_term(value)
  for value in _DEFAULT_TAGS_BY_KEY.keys()
}
_KNOWN_INFER_KEYS.update(
  _normalize_term(value)
  for value in _KEY_ALIAS_MAP.values()
)
_KNOWN_INFER_KEYS.discard("")


def _canonicalize_key(value: Any) -> str:
  token = _normalize_token(value, max_len=MAX_KEY_LEN)
  if not token:
    return ""
  return _KEY_ALIAS_MAP.get(token, token)


def _normalize_tags(value: Any) -> list[str]:
  if not isinstance(value, list):
    return []
  tags: list[str] = []
  seen: set[str] = set()
  for raw in value[: MAX_TAGS * 4]:
    tag = _normalize_term(raw)
    if not tag:
      continue
    canonical = _KEY_ALIAS_MAP.get(tag, tag)
    canonical = _normalize_token(canonical, max_len=MAX_TAG_LEN)
    if not canonical or canonical in seen:
      continue
    seen.add(canonical)
    tags.append(canonical)
    if len(tags) >= MAX_TAGS:
      break
  return tags


def _merge_tags(left: list[str], right: list[str]) -> list[str]:
  merged: list[str] = []
  seen: set[str] = set()
  for source in [left, right]:
    for tag in source:
      safe_tag = _normalize_term(tag)
      if not safe_tag:
        continue
      safe_tag = _normalize_token(_KEY_ALIAS_MAP.get(safe_tag, safe_tag), max_len=MAX_TAG_LEN)
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
  safe = _normalize_text(value, max_len=320).lower()
  if not safe:
    return []
  tokens = re.findall(r"[^\W_]{2,}", safe, flags=re.UNICODE)
  deduped: list[str] = []
  seen: set[str] = set()
  for token in tokens:
    safe_token = _normalize_term(token)
    if not safe_token or safe_token in seen or safe_token in _QUERY_STOPWORDS:
      continue
    seen.add(safe_token)
    deduped.append(safe_token)
    if len(deduped) >= 16:
      break
  return deduped


def _expand_query_terms(terms: list[str]) -> list[str]:
  expanded: list[str] = []
  seen: set[str] = set()
  for raw in terms:
    term = _normalize_term(raw)
    if not term:
      continue
    for candidate in [term, *_QUERY_SYNONYM_ALIASES.get(term, set())]:
      safe_candidate = _normalize_term(candidate)
      if not safe_candidate or safe_candidate in seen:
        continue
      seen.add(safe_candidate)
      expanded.append(safe_candidate)
  return expanded


def _infer_key_from_terms(terms: list[str]) -> str:
  for term in terms:
    token = _normalize_token(term, max_len=MAX_KEY_LEN)
    if not token:
      continue
    mapped = _normalize_term(_KEY_ALIAS_MAP.get(token, token))
    if mapped in _KNOWN_INFER_KEYS:
      return mapped
  return ""


def _looks_like_generic_recall_query(value: str) -> bool:
  query = _normalize_text(value, max_len=240).lower()
  if not query:
    return False
  for pattern in _GENERIC_RECALL_PATTERNS:
    if re.search(pattern, query, flags=re.IGNORECASE):
      return True

  # If only memory-intent verbs remain after stopword filtering, treat it as a generic recall query.
  intent_terms = set(_tokenize_query(query))
  if not intent_terms:
    return True
  return intent_terms.issubset({
    "помнишь",
    "помнить",
    "знаешь",
    "remember",
    "recall",
    "memory",
  })


def _infer_slot_from_text(text: str) -> tuple[str, list[str]]:
  safe = _normalize_text(text, max_len=MAX_FACT_LEN).lower()
  if not safe:
    return "", []
  for slot_key, pattern_list, tags in _SLOT_HINT_RULES:
    for pattern in pattern_list:
      if re.search(pattern, safe, flags=re.IGNORECASE):
        return slot_key, list(tags)
  return "", []


def _default_tags_for_key(key: str) -> list[str]:
  safe_key = _canonicalize_key(key)
  if not safe_key:
    return []
  return _normalize_tags(_DEFAULT_TAGS_BY_KEY.get(safe_key, []))


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


def _build_lexical_blob(entry: dict[str, Any]) -> str:
  fact = _normalize_text(entry.get("fact"), max_len=MAX_FACT_LEN)
  key = _canonicalize_key(entry.get("key"))
  tags = _normalize_tags(entry.get("tags"))
  tokens: list[str] = []

  for source in [fact, key, " ".join(tags)]:
    if source:
      tokens.extend(_tokenize_query(source))

  expanded = _expand_query_terms(tokens)
  pieces = [fact, key, " ".join(tags), " ".join(expanded)]
  return _normalize_text(" ".join(part for part in pieces if part), max_len=4000)


def _build_sparse_vector(text: str) -> dict[str, float]:
  terms = _expand_query_terms(_tokenize_query(text))
  if not terms:
    return {}
  weights: dict[str, float] = {}
  for term in terms:
    key = f"t:{term}"
    weights[key] = float(weights.get(key, 0.0) + 1.0)
    if len(term) >= 4:
      for idx in range(len(term) - 2):
        gram = term[idx: idx + 3]
        g_key = f"g:{gram}"
        weights[g_key] = float(weights.get(g_key, 0.0) + 0.2)
  if len(weights) > MAX_VECTOR_TERMS:
    top_items = sorted(weights.items(), key=lambda item: item[1], reverse=True)[:MAX_VECTOR_TERMS]
    return {key: value for key, value in top_items}
  return weights


def _cosine_similarity(left: dict[str, float], right: dict[str, float]) -> float:
  if not left or not right:
    return 0.0
  dot = 0.0
  left_norm = 0.0
  right_norm = 0.0
  for key, value in left.items():
    left_norm += value * value
    dot += value * float(right.get(key, 0.0))
  for value in right.values():
    right_norm += value * value
  if left_norm <= 0.0 or right_norm <= 0.0:
    return 0.0
  return dot / (math.sqrt(left_norm) * math.sqrt(right_norm))


def _fuzzy_similarity(query: str, target: str) -> float:
  q = _normalize_text(query, max_len=220).lower()
  t = _normalize_text(target, max_len=420).lower()
  if not q or not t:
    return 0.0
  return SequenceMatcher(a=q, b=t).ratio()


def _latinize_cyrillic(value: str) -> str:
  chars: list[str] = []
  for ch in value:
    chars.append(_CYRILLIC_TO_LATIN.get(ch, ch))
  return "".join(chars)


def _normalize_user_identity(value: Any) -> str:
  raw = _normalize_text(value, max_len=96).lower()
  if not raw:
    return ""
  latin = _latinize_cyrillic(raw)
  latin = re.sub(r"[^a-z0-9]+", " ", latin, flags=re.IGNORECASE)
  return re.sub(r"\s+", " ", latin).strip()


def _split_user_identity(value: Any) -> list[str]:
  normalized = _normalize_user_identity(value)
  if not normalized:
    return []
  parts = [part for part in normalized.split(" ") if len(part) >= 2]
  deduped: list[str] = []
  seen: set[str] = set()
  for part in parts:
    if part in seen:
      continue
    seen.add(part)
    deduped.append(part)
    if len(deduped) >= 6:
      break
  return deduped


def _user_identities_match(runtime_user_name: str, entry_user_name: str) -> bool:
  if not runtime_user_name or not entry_user_name:
    return False
  if runtime_user_name == entry_user_name:
    return True

  runtime_norm = _normalize_user_identity(runtime_user_name)
  entry_norm = _normalize_user_identity(entry_user_name)
  if not runtime_norm or not entry_norm:
    return False
  if runtime_norm == entry_norm:
    return True

  if len(runtime_norm) >= 4 and runtime_norm in entry_norm:
    return True
  if len(entry_norm) >= 4 and entry_norm in runtime_norm:
    return True

  runtime_tokens = _split_user_identity(runtime_user_name)
  entry_tokens = _split_user_identity(entry_user_name)
  if runtime_tokens and entry_tokens:
    runtime_set = set(runtime_tokens)
    entry_set = set(entry_tokens)
    overlap = runtime_set.intersection(entry_set)
    if overlap and (len(overlap) / float(min(len(runtime_set), len(entry_set)))) >= 0.5:
      return True
    # Handle transliteration variants like "andrey" vs "andrei".
    for left in runtime_tokens:
      for right in entry_tokens:
        if SequenceMatcher(a=left, b=right).ratio() >= 0.78:
          return True

  return SequenceMatcher(a=runtime_norm, b=entry_norm).ratio() >= 0.72


def _normalize_memory_entry(raw: Any) -> dict[str, Any] | None:
  if not isinstance(raw, dict):
    return None
  fact = _normalize_text(raw.get("fact"), max_len=MAX_FACT_LEN)
  if not fact:
    return None
  entry_id = _normalize_text(raw.get("id"), max_len=120) or _new_memory_id()
  key = _canonicalize_key(raw.get("key"))
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


def _entry_sort_key(entry: dict[str, Any]) -> tuple[float, int]:
  return (_parse_iso(entry.get("updated_at")).timestamp(), int(entry.get("importance") or 3))


def _prepare_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
  return cleaned


def _get_sqlite_handles(host: Any) -> tuple[Any, sqlite3.Connection, Any] | None:
  storage = getattr(host, "storage", None)
  conn = getattr(storage, "_conn", None)
  lock = getattr(storage, "_lock", None)
  if storage is None or not isinstance(conn, sqlite3.Connection) or lock is None:
    return None
  return storage, conn, lock


def _replace_sqlite_entries_locked(conn: sqlite3.Connection, entries: list[dict[str, Any]], *, fts_enabled: bool) -> None:
  conn.execute(f"DELETE FROM {SQL_TABLE}")
  if entries:
    payloads: list[tuple[Any, ...]] = []
    for entry in entries:
      lexical_blob = _build_lexical_blob(entry)
      semantic_json = json.dumps(_build_sparse_vector(lexical_blob), ensure_ascii=False, separators=(",", ":"))
      payloads.append(
        (
          str(entry.get("id") or ""),
          _canonicalize_key(entry.get("key")),
          _normalize_text(entry.get("fact"), max_len=MAX_FACT_LEN),
          json.dumps(_normalize_tags(entry.get("tags")), ensure_ascii=False),
          _safe_int(entry.get("importance"), fallback=3, min_value=1, max_value=5),
          _normalize_text(entry.get("created_at"), max_len=64) or _now_utc_iso(),
          _normalize_text(entry.get("updated_at"), max_len=64) or _now_utc_iso(),
          _normalize_text(entry.get("user_name"), max_len=96),
          _normalize_text(entry.get("chat_id"), max_len=96),
          lexical_blob,
          semantic_json,
        )
      )
    conn.executemany(
      f"""
      INSERT INTO {SQL_TABLE}(
        id, key, fact, tags_json, importance, created_at, updated_at, user_name, chat_id, lexical_blob, semantic_json
      ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      payloads,
    )

  if fts_enabled:
    conn.execute(f"DELETE FROM {SQL_FTS_TABLE}")
    if entries:
      fts_payloads = [
        (
          str(entry.get("id") or ""),
          _normalize_text(entry.get("fact"), max_len=MAX_FACT_LEN),
          _canonicalize_key(entry.get("key")),
          " ".join(_normalize_tags(entry.get("tags"))),
          _build_lexical_blob(entry),
        )
        for entry in entries
      ]
      conn.executemany(
        f"""
        INSERT INTO {SQL_FTS_TABLE}(id, fact, key, tags, lexical_blob)
        VALUES(?, ?, ?, ?, ?)
        """,
        fts_payloads,
      )


def _ensure_sqlite_schema(host: Any) -> bool:
  global _SQLITE_SCHEMA_READY
  global _SQLITE_FTS_ENABLED
  global _SQLITE_UNAVAILABLE

  if _SQLITE_UNAVAILABLE:
    return False
  if _SQLITE_SCHEMA_READY:
    return True

  handles = _get_sqlite_handles(host)
  if handles is None:
    _SQLITE_UNAVAILABLE = True
    return False
  storage, conn, lock = handles

  migration_done = bool(storage.get_setting_flag(SQLITE_MIGRATION_FLAG_KEY, False))
  migration_seed = storage.get_setting_json(STORAGE_KEY, []) if not migration_done else []

  try:
    with lock, conn:
      conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SQL_TABLE} (
          id TEXT PRIMARY KEY,
          key TEXT NOT NULL DEFAULT '',
          fact TEXT NOT NULL,
          tags_json TEXT NOT NULL DEFAULT '[]',
          importance INTEGER NOT NULL DEFAULT 3,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          user_name TEXT NOT NULL DEFAULT '',
          chat_id TEXT NOT NULL DEFAULT '',
          lexical_blob TEXT NOT NULL DEFAULT '',
          semantic_json TEXT NOT NULL DEFAULT '{{}}'
        )
        """
      )
      conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{SQL_TABLE}_user_updated ON {SQL_TABLE}(user_name, updated_at)")
      conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{SQL_TABLE}_key ON {SQL_TABLE}(key)")
      conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{SQL_TABLE}_chat ON {SQL_TABLE}(chat_id)")

      fts_enabled = False
      try:
        conn.execute(
          f"""
          CREATE VIRTUAL TABLE IF NOT EXISTS {SQL_FTS_TABLE}
          USING fts5(id UNINDEXED, fact, key, tags, lexical_blob)
          """
        )
        fts_enabled = True
      except sqlite3.Error:
        fts_enabled = False

      if not migration_done:
        row = conn.execute(f"SELECT COUNT(1) AS c FROM {SQL_TABLE}").fetchone()
        existing_count = int(row["c"] if row else 0)
        if existing_count <= 0 and isinstance(migration_seed, list):
          seed_entries = _prepare_entries([
            entry
            for entry in (
              _normalize_memory_entry(item)
              for item in migration_seed
            )
            if entry is not None
          ])
          _replace_sqlite_entries_locked(conn, seed_entries, fts_enabled=fts_enabled)

      _SQLITE_FTS_ENABLED = fts_enabled
      _SQLITE_SCHEMA_READY = True

    if not migration_done:
      storage.set_setting_flag(SQLITE_MIGRATION_FLAG_KEY, True)
    return True
  except sqlite3.Error:
    _SQLITE_UNAVAILABLE = True
    return False


def _load_entries_from_settings(host: Any) -> list[dict[str, Any]]:
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
  return _prepare_entries(entries)


def _save_entries_to_settings(host: Any, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
  cleaned = _prepare_entries(entries)
  host.storage.set_setting_json(STORAGE_KEY, cleaned[:JSON_MIRROR_MAX])
  return cleaned


def _load_entries_from_sqlite(host: Any) -> list[dict[str, Any]] | None:
  if not _ensure_sqlite_schema(host):
    return None
  handles = _get_sqlite_handles(host)
  if handles is None:
    return None
  _storage, conn, lock = handles
  try:
    with lock:
      rows = conn.execute(
        f"""
        SELECT id, key, fact, tags_json, importance, created_at, updated_at, user_name, chat_id, lexical_blob, semantic_json
        FROM {SQL_TABLE}
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (MAX_ENTRIES,),
      ).fetchall()
  except sqlite3.Error:
    return None

  entries: list[dict[str, Any]] = []
  for row in rows:
    try:
      tags = json.loads(str(row["tags_json"] or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
      tags = []
    try:
      semantic_vector = json.loads(str(row["semantic_json"] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
      semantic_vector = {}
    if not isinstance(semantic_vector, dict):
      semantic_vector = {}

    entry = _normalize_memory_entry(
      {
        "id": row["id"],
        "key": row["key"],
        "fact": row["fact"],
        "tags": tags,
        "importance": row["importance"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "user_name": row["user_name"],
        "chat_id": row["chat_id"],
      }
    )
    if not entry:
      continue
    lexical_blob = _normalize_text(row["lexical_blob"], max_len=4000)
    if lexical_blob:
      entry["_lexical_blob"] = lexical_blob
    if semantic_vector:
      entry["_semantic_vector"] = {
        str(key): float(value)
        for key, value in semantic_vector.items()
        if isinstance(key, str) and isinstance(value, (int, float))
      }
    entries.append(entry)
  return entries


def _save_entries_to_sqlite(host: Any, entries: list[dict[str, Any]]) -> list[dict[str, Any]] | None:
  if not _ensure_sqlite_schema(host):
    return None
  handles = _get_sqlite_handles(host)
  if handles is None:
    return None
  storage, conn, lock = handles
  cleaned = _prepare_entries(entries)
  try:
    with lock, conn:
      _replace_sqlite_entries_locked(conn, cleaned, fts_enabled=_SQLITE_FTS_ENABLED)
  except sqlite3.Error:
    return None

  storage.set_setting_json(STORAGE_KEY, cleaned[:JSON_MIRROR_MAX])
  return cleaned


def _load_entries(host: Any) -> list[dict[str, Any]]:
  sqlite_entries = _load_entries_from_sqlite(host)
  if sqlite_entries is not None:
    if sqlite_entries:
      return sqlite_entries
    fallback_entries = _load_entries_from_settings(host)
    if fallback_entries:
      saved = _save_entries_to_sqlite(host, fallback_entries)
      if saved is not None:
        refreshed = _load_entries_from_sqlite(host)
        if refreshed is not None:
          return refreshed
    return fallback_entries
  return _load_entries_from_settings(host)


def _save_entries(host: Any, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
  saved = _save_entries_to_sqlite(host, entries)
  if saved is not None:
    return saved
  return _save_entries_to_settings(host, entries)


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
  if not entry_user:
    return True
  return _user_identities_match(safe_runtime_user, entry_user)


def _public_memory(entry: dict[str, Any], *, include_user: bool) -> dict[str, Any]:
  payload: dict[str, Any] = {
    "id": str(entry.get("id") or ""),
    "key": _canonicalize_key(entry.get("key")),
    "fact": str(entry.get("fact") or ""),
    "tags": _normalize_tags(entry.get("tags")),
    "importance": int(entry.get("importance") or 3),
    "updated_at": str(entry.get("updated_at") or ""),
  }
  if include_user:
    payload["user_name"] = str(entry.get("user_name") or "")
  return payload


def _entry_vector(entry: dict[str, Any]) -> dict[str, float]:
  raw = entry.get("_semantic_vector")
  if isinstance(raw, dict):
    vector = {
      str(key): float(value)
      for key, value in raw.items()
      if isinstance(key, str) and isinstance(value, (int, float))
    }
    if vector:
      return vector
  blob = _normalize_text(entry.get("_lexical_blob"), max_len=4000)
  if not blob:
    blob = _build_lexical_blob(entry)
  return _build_sparse_vector(blob)


def _entry_search_blob(entry: dict[str, Any]) -> str:
  blob = _normalize_text(entry.get("_lexical_blob"), max_len=4000)
  if blob:
    return blob.lower()
  return _build_lexical_blob(entry).lower()


def _fts_match_query(terms: list[str]) -> str:
  parts: list[str] = []
  for term in terms:
    safe_term = _normalize_term(term)
    if len(safe_term) < 2:
      continue
    safe_term = re.sub(r"[^a-zа-я0-9_\-]+", "", safe_term, flags=re.IGNORECASE)
    if not safe_term:
      continue
    parts.append(f"{safe_term}*")
  return " OR ".join(parts[:12])


def _search_fts_bonus_map(host: Any, base_terms: list[str], *, limit: int = 120) -> dict[str, float]:
  if not base_terms or not _ensure_sqlite_schema(host) or not _SQLITE_FTS_ENABLED:
    return {}
  handles = _get_sqlite_handles(host)
  if handles is None:
    return {}
  _storage, conn, lock = handles

  match_query = _fts_match_query(base_terms)
  if not match_query:
    return {}

  try:
    with lock:
      rows = conn.execute(
        f"""
        SELECT id, bm25({SQL_FTS_TABLE}) AS rank
        FROM {SQL_FTS_TABLE}
        WHERE {SQL_FTS_TABLE} MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (match_query, max(8, min(200, int(limit)))),
      ).fetchall()
  except sqlite3.Error:
    return {}

  bonuses: dict[str, float] = {}
  for index, row in enumerate(rows):
    memory_id = _normalize_text(row["id"], max_len=120)
    if not memory_id:
      continue
    rank = row["rank"]
    try:
      rank_value = float(rank)
    except (TypeError, ValueError):
      rank_value = 0.0
    base_bonus = max(0.0, 24.0 - index * 0.45)
    if rank_value > 0:
      base_bonus += max(0.0, 4.0 / (1.0 + rank_value))
    elif rank_value < 0:
      base_bonus += min(4.0, abs(rank_value))
    if base_bonus <= 0.0:
      continue
    bonuses[memory_id] = max(bonuses.get(memory_id, 0.0), base_bonus)
  return bonuses


def remember(args: dict[str, Any], runtime: Any, host: Any) -> dict[str, Any]:
  payload = args or {}
  fact = _normalize_text(payload.get("fact"), max_len=MAX_FACT_LEN)
  if not fact:
    raise ValueError("fact is required")

  key = _canonicalize_key(payload.get("key"))
  tags = _normalize_tags(payload.get("tags"))
  importance = _safe_int(payload.get("importance"), fallback=3, min_value=1, max_value=5)
  overwrite_key = bool(payload.get("overwrite_key", True))

  inferred_key, inferred_tags = _infer_slot_from_text(fact)
  if not key and inferred_key:
    key = inferred_key
  tags = _merge_tags(tags, inferred_tags)
  if key:
    tags = _merge_tags(tags, _default_tags_for_key(key))

  runtime_user_name = _normalize_text(getattr(runtime, "user_name", ""), max_len=96)
  runtime_chat_id = _normalize_text(getattr(runtime, "chat_id", ""), max_len=96)
  now = _now_utc_iso()

  entries = _load_entries(host)
  target_index = -1

  fact_lc = fact.lower()
  for index, entry in enumerate(entries):
    if not _matches_scope(entry, scope="current_user", runtime_user_name=runtime_user_name):
      continue
    if _normalize_text(entry.get("fact"), max_len=MAX_FACT_LEN).lower() == fact_lc:
      target_index = index
      break

  if target_index < 0 and key and overwrite_key:
    for index, entry in enumerate(entries):
      if not _matches_scope(entry, scope="current_user", runtime_user_name=runtime_user_name):
        continue
      if _canonicalize_key(entry.get("key")) == key:
        target_index = index
        break

  action = "saved"
  if target_index >= 0:
    existing = dict(entries[target_index])
    existing_tags = _normalize_tags(existing.get("tags"))
    merged_tags = _merge_tags(existing_tags, tags)
    existing["fact"] = fact
    existing["key"] = key or _canonicalize_key(existing.get("key"))
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
  query = _normalize_text(payload.get("query"), max_len=220)
  if _looks_like_generic_recall_query(query):
    query = ""
  key = _canonicalize_key(payload.get("key"))
  tags = _normalize_tags(payload.get("tags"))
  scope = _resolve_scope(payload.get("scope"))
  limit = _safe_int(payload.get("limit"), fallback=5, min_value=1, max_value=20)

  base_query_terms = _tokenize_query(query)
  query_terms = _expand_query_terms(base_query_terms)
  if not key:
    key = _infer_key_from_terms(base_query_terms + query_terms)

  runtime_user_name = _normalize_text(getattr(runtime, "user_name", ""), max_len=96)
  include_user = scope == "all"
  entries = _load_entries(host)

  fts_bonus_map = _search_fts_bonus_map(host, base_query_terms)
  query_vector = _build_sparse_vector(" ".join(query_terms if query_terms else base_query_terms))
  query_lower = query.lower()
  now_ts = dt.datetime.now(dt.timezone.utc).timestamp()

  ranked: list[tuple[float, float, dict[str, Any]]] = []
  for entry in entries:
    if not _matches_scope(entry, scope=scope, runtime_user_name=runtime_user_name):
      continue

    entry_key = _canonicalize_key(entry.get("key"))
    entry_tags = _normalize_tags(entry.get("tags"))
    if key and entry_key != key:
      continue
    if tags and not set(tags).issubset(set(entry_tags)):
      continue

    entry_id = _normalize_text(entry.get("id"), max_len=120)
    searchable_fact = _normalize_text(entry.get("fact"), max_len=MAX_FACT_LEN).lower()
    searchable_key = entry_key.lower()
    searchable_tags = [str(tag).lower() for tag in entry_tags]
    search_blob = _entry_search_blob(entry)

    score = float(int(entry.get("importance") or 3)) * 2.2
    if key and entry_key == key:
      score += 85.0
    if tags:
      score += 12.0
    score += float(fts_bonus_map.get(entry_id, 0.0))

    updated_ts = _parse_iso(entry.get("updated_at")).timestamp()
    age_days = max(0.0, (now_ts - updated_ts) / 86400.0)
    score += max(0.0, 8.0 - min(8.0, age_days * 0.08))

    lexical_hits = 0
    if query_terms:
      for term in query_terms:
        if term in searchable_key:
          score += 18.0
          lexical_hits += 1
        if term in searchable_fact:
          score += 12.0
          lexical_hits += 1
        if any(term in tag for tag in searchable_tags):
          score += 10.0
          lexical_hits += 1
        if term in search_blob:
          score += 6.0
          lexical_hits += 1

      semantic_score = _cosine_similarity(query_vector, _entry_vector(entry))
      score += semantic_score * 28.0

      fuzzy_target = f"{searchable_fact} {searchable_key} {' '.join(searchable_tags)}".strip()
      fuzzy_score = _fuzzy_similarity(query_lower, fuzzy_target)
      score += fuzzy_score * 12.0

      fts_bonus = float(fts_bonus_map.get(entry_id, 0.0))
      if lexical_hits == 0 and semantic_score < 0.08 and fuzzy_score < 0.26 and fts_bonus <= 0.0:
        continue

    ranked.append((score, updated_ts, entry))

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
  key = _canonicalize_key(payload.get("key"))
  query = _normalize_text(payload.get("query"), max_len=220)
  scope = _resolve_scope(payload.get("scope"))
  all_matching = bool(payload.get("all_matching", False))
  base_query_terms = _tokenize_query(query)
  expanded_query_terms = _expand_query_terms(base_query_terms)

  if not target_id and not key and not base_query_terms:
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
    entry_key = _canonicalize_key(entry.get("key"))
    search_blob = _entry_search_blob(entry)

    matches = False
    if target_id and entry_id == target_id:
      matches = True
    if key and entry_key == key:
      matches = True
    if base_query_terms and all(term in search_blob for term in base_query_terms):
      matches = True
    elif expanded_query_terms and any(term in search_blob for term in expanded_query_terms):
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
