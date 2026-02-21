from __future__ import annotations

from typing import Any


def handle(args: dict[str, Any], runtime: Any, host: Any) -> dict[str, Any]:
  requested_mood = str((args or {}).get("mood") or getattr(runtime, "mood", "") or "").strip()
  fallback_mood = str(getattr(runtime, "mood", "") or "neutral").strip() or "neutral"
  chat_id = str(getattr(runtime, "chat_id", "") or "").strip() or "default"
  mood = host.normalize_mood(requested_mood, fallback_mood)
  host.update_chat_mood(chat_id=chat_id, mood=mood)
  return {"chat_id": chat_id, "mood": mood}
