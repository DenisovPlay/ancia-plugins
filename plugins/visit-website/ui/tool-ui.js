(function registerVisitWebsiteToolUi() {
  const api = window.AnciaPluginUI;
  if (!api || typeof api.registerToolRenderer !== "function") {
    return;
  }

  function normalizeText(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
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

  function truncate(value, maxLen) {
    const safe = normalizeText(value);
    if (!safe) {
      return "";
    }
    if (safe.length <= maxLen) {
      return safe;
    }
    return `${safe.slice(0, Math.max(0, maxLen - 1))}…`;
  }

  api.registerToolRenderer({
    pluginId: "visit-website",
    toolName: "web.visit.website",
    getQueryPreview({ args, output }) {
      const rawUrl = String(args?.url || output?.requested_url || output?.url || "").trim();
      if (!rawUrl) {
        return "Открытие страницы";
      }
      try {
        const parsed = new URL(rawUrl);
        return parsed.hostname;
      } catch {
        return truncate(rawUrl, 56);
      }
    },
    formatStart({ args }) {
      const rawUrl = String(args?.url || "").trim();
      const safeUrl = toSafeMarkdownUrl(rawUrl);
      if (safeUrl) {
        return `**Открываю страницу:** ${safeUrl}`;
      }
      return "_Открываю страницу..._";
    },
    formatOutput({ output }) {
      if (!output || typeof output !== "object") {
        return "";
      }
      if (output.error) {
        return `**Ошибка открытия страницы:** ${truncate(output.error, 400)}`;
      }

      const title = truncate(output.title || "", 220);
      const pageUrl = toSafeMarkdownUrl(output.url || output.requested_url || "");
      const content = truncate(output.content || "", 2600);
      const links = Array.isArray(output.links) ? output.links : [];

      const lines = [];
      if (title && pageUrl) {
        lines.push(`**Страница:** [${title}](${pageUrl})`);
      } else if (pageUrl) {
        lines.push(`**Страница:** ${pageUrl}`);
      } else if (title) {
        lines.push(`**Страница:** ${title}`);
      } else {
        lines.push("**Страница открыта**");
      }

      if (content) {
        lines.push("");
        lines.push(content);
      }

      const safeLinks = links
        .map((item) => toSafeMarkdownUrl(item))
        .filter(Boolean)
        .slice(0, 12);

      if (safeLinks.length > 0) {
        lines.push("");
        lines.push("**Ссылки на странице**");
        safeLinks.forEach((link) => {
          let label = link;
          try {
            const parsed = new URL(link);
            label = truncate(`${parsed.hostname}${parsed.pathname || ""}`, 80) || parsed.hostname;
          } catch {
            // Keep raw url label.
          }
          lines.push(`- [${label}](${link})`);
        });
      }

      return lines.join("\n").trim();
    },
  });
})();
