from __future__ import annotations

from typing import Any


def handle(args: dict[str, Any], runtime: Any, host: Any) -> dict[str, Any]:
  timezone = str(getattr(runtime, "timezone", "") or "UTC").strip() or "UTC"
  local_time, tz = host.now_for_timezone(timezone)
  return {
    "local_time": local_time,
    "timezone": tz,
    "request_id": host.create_request_id(),
  }
