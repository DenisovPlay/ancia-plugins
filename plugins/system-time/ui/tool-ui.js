(function registerSystemTimeToolUi() {
  const api = window.AnciaPluginUI;
  if (!api || typeof api.registerToolRenderer !== "function") {
    return;
  }

  function normalizeText(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }

  api.registerToolRenderer({
    pluginId: "system-time",
    toolName: "system.time",
    getQueryPreview() {
      return "Текущее время";
    },
    formatStart() {
      return "_Получаю текущее время..._";
    },
    formatOutput({ output }) {
      if (!output || typeof output !== "object") {
        return "";
      }
      if (output.error) {
        return `**Ошибка времени:** ${normalizeText(output.error)}`;
      }
      const localTime = normalizeText(output.local_time || output.time || output.datetime || "");
      const timezone = normalizeText(output.timezone || output.tz || "");
      const lines = [];
      if (localTime) {
        lines.push(`**Локальное время:** ${localTime}`);
      }
      if (timezone) {
        lines.push(`**Часовой пояс:** ${timezone}`);
      }
      return lines.join("\n") || "**Время получено.**";
    },
  });
})();
