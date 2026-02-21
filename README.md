# Ancia Plugins Registry

Официальный реестр плагинов для Ancia.

## Registry URL

- `https://raw.githubusercontent.com/DenisovPlay/ancia-plugins/main/index.json`

## Структура

- `index.json` — реестр плагинов для backend Ancia.
- `plugins/<plugin-id>/` — исходники плагина (`manifest.json`, `plugin.py`, `ui/*`).
- `packages/*.zip` — архивы плагинов для сетевой установки/обновления.

## Предустановленные плагины

- `duckduckgo`
- `visit-website`
- `system-time`
- `chat-mood`

## Обновление пакетов

```bash
./scripts/build_packages.sh
```

После сборки проверьте `index.json` и закоммитьте изменения.
