(function registerDuckDuckGoToolUi() {
  const api = window.AnciaPluginUI;
  if (!api || typeof api.registerToolRenderer !== "function") {
    return;
  }

  function normalizeText(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }

  function sanitizeTitle(value, fallback) {
    const normalized = normalizeText(value).replace(/[\[\]]+/g, "").trim();
    return normalized || fallback;
  }

  function toSafeMarkdownUrl(value) {
    const raw = String(value || "").trim();
    if (!raw) {
      return "";
    }
    try {
      const parsed = new URL(raw);
      if (!/^https?:$/i.test(parsed.protocol)) {
        return "";
      }
      return parsed
        .toString()
        .replace(/\(/g, "%28")
        .replace(/\)/g, "%29");
    } catch {
      return "";
    }
  }

  function formatResultLine(item, index) {
    const title = sanitizeTitle(item?.title, `Результат ${index + 1}`);
    const safeUrl = toSafeMarkdownUrl(item?.url);
    const snippet = normalizeText(item?.snippet || item?.description || "");
    const shortSnippet = snippet ? ` — ${snippet.length > 220 ? `${snippet.slice(0, 219)}…` : snippet}` : "";
    if (safeUrl) {
      return `${index + 1}. [${title}](${safeUrl})${shortSnippet}`;
    }
    return `${index + 1}. ${title}${shortSnippet}`;
  }

  api.registerToolRenderer({
    pluginId: "duckduckgo",
    toolName: "web.search.duckduckgo",
    getQueryPreview({ args, output }) {
      const query = normalizeText(args?.query || output?.query || "");
      return query ? `Поиск: ${query}` : "Поиск";
    },
    formatStart({ args }) {
      const query = normalizeText(args?.query || "");
      if (query) {
        return `**Поиск:** ${query}`;
      }
      return "_Запуск веб-поиска..._";
    },
    formatOutput({ output }) {
      if (!output || typeof output !== "object") {
        return "";
      }
      if (output.error) {
        return `**Ошибка поиска:** ${normalizeText(output.error)}`;
      }

      const results = Array.isArray(output.results) ? output.results : [];
      const lines = ["**Результаты поиска**", ""];
      if (!results.length) {
        lines.push("_По запросу ничего не найдено._");
        return lines.join("\n");
      }

      results.slice(0, 10).forEach((item, index) => {
        lines.push(formatResultLine(item, index));
      });
      return lines.join("\n");
    },
  });
})();
