#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLUGINS_DIR="$ROOT_DIR/plugins"
PACKAGES_DIR="$ROOT_DIR/packages"

mkdir -p "$PACKAGES_DIR"

while IFS= read -r -d '' manifest; do
  plugin_dir="$(dirname "$manifest")"
  plugin_id="$(basename "$plugin_dir")"
  version="$(python3 - <<PY
import json
from pathlib import Path
payload = json.loads(Path(r'''$manifest''').read_text(encoding='utf-8'))
print(str(payload.get('version') or '0.1.0').strip())
PY
)"
  package_path="$PACKAGES_DIR/${plugin_id}-${version}.zip"
  rm -f "$package_path"
  (
    cd "$PLUGINS_DIR"
    zip -r "$package_path" "$plugin_id" >/dev/null
  )
  echo "built: $package_path"
done < <(find "$PLUGINS_DIR" -mindepth 2 -maxdepth 2 -name manifest.json -print0)
