(function registerChatMoodToolUi() {
  const api = window.AnciaPluginUI;
  if (!api || typeof api.registerToolRenderer !== "function") {
    return;
  }

  const MOOD_LABELS = {
    neutral: "Нейтральное",
    success: "Успех",
    error: "Ошибка",
    warning: "Предупреждение",
    thinking: "Размышление",
    planning: "Планирование",
    coding: "Разработка",
    researching: "Исследование",
    creative: "Творчество",
    friendly: "Дружелюбное",
    waiting: "Ожидание",
    offline: "Офлайн",
  };

  function normalizeText(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }

  function moodLabel(value) {
    const key = normalizeText(value).toLowerCase();
    return MOOD_LABELS[key] || key || "Не указано";
  }

  api.registerToolRenderer({
    pluginId: "chat-mood",
    toolName: "chat.set_mood",
    getQueryPreview({ args, output }) {
      const mood = normalizeText(output?.mood || args?.mood || "").toLowerCase();
      return mood ? `Режим: ${mood}` : "Смена состояния";
    },
    formatStart({ args }) {
      const mood = normalizeText(args?.mood || "").toLowerCase();
      if (!mood) {
        return "_Обновляю визуальное состояние чата..._";
      }
      return `**Смена состояния:** ${moodLabel(mood)}`;
    },
    formatOutput({ output, args }) {
      if (!output || typeof output !== "object") {
        return "";
      }
      if (output.error) {
        return `**Ошибка смены состояния:** ${normalizeText(output.error)}`;
      }
      const mood = normalizeText(output?.mood || args?.mood || "").toLowerCase();
      const chatId = normalizeText(output?.chat_id || "");
      const lines = [`**Состояние чата:** ${moodLabel(mood)}`];
      if (chatId) {
        lines.push(`**Чат:** ${chatId}`);
      }
      return lines.join("\n");
    },
  });
})();
