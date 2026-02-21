(function registerUserMemoryToolUi() {
  function normalizeText(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
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

  function safeList(value) {
    return Array.isArray(value) ? value : [];
  }

  function registerVariants(toolNames, renderer) {
    const api = window.AnciaPluginUI;
    if (!api || typeof api.registerToolRenderer !== "function") {
      return false;
    }
    safeList(toolNames).forEach((toolNameRaw) => {
      const toolName = normalizeText(toolNameRaw).toLowerCase();
      if (!toolName) {
        return;
      }
      api.registerToolRenderer({
        pluginId: "user-memory",
        ...renderer,
        toolName,
      });
    });
    return true;
  }

  function formatMemoryLine(entry, index) {
    const fact = truncate(entry?.fact || "", 180) || "Без текста";
    const key = normalizeText(entry?.key || "");
    const tags = safeList(entry?.tags).map((item) => normalizeText(item)).filter(Boolean);
    const bits = [];
    if (key) {
      bits.push(`key=${key}`);
    }
    if (tags.length > 0) {
      bits.push(`tags=${tags.join(", ")}`);
    }
    return `${index + 1}. ${fact}${bits.length ? ` (${bits.join(" • ")})` : ""}`;
  }

  function registerAll() {
    const rememberOk = registerVariants(["memory.remember", "memory.user.remember"], {
    getQueryPreview({ args, output }) {
      const fact = normalizeText(args?.fact || output?.memory?.fact || "");
      return fact ? `Запомнить: ${truncate(fact, 56)}` : "Сохранение памяти";
    },
    formatStart({ args }) {
      const fact = normalizeText(args?.fact || "");
      if (!fact) {
        return "_Сохраняю память о пользователе..._";
      }
      return `**Запоминаю:** ${truncate(fact, 240)}`;
    },
    formatOutput({ output }) {
      if (!output || typeof output !== "object") {
        return "";
      }
      if (output.error) {
        return `**Ошибка сохранения памяти:** ${truncate(output.error, 400)}`;
      }

      const status = normalizeText(output.status || "saved").toLowerCase();
      const actionLabel = status === "updated" ? "Память обновлена" : "Память сохранена";
      const memory = output.memory && typeof output.memory === "object" ? output.memory : {};
      const fact = truncate(memory.fact || "", 380);
      const key = normalizeText(memory.key || "");
      const tags = safeList(memory.tags).map((item) => normalizeText(item)).filter(Boolean);
      const lines = [`**${actionLabel}**`];

      if (fact) {
        lines.push(`- Факт: ${fact}`);
      }
      if (key) {
        lines.push(`- Ключ: ${key}`);
      }
      if (tags.length > 0) {
        lines.push(`- Теги: ${tags.join(", ")}`);
      }
      if (typeof output.total_memories === "number") {
        lines.push(`- Всего записей: ${Math.max(0, Number(output.total_memories) || 0)}`);
      }
      const requestId = normalizeText(output.request_id || "");
      if (requestId) {
        lines.push(`- ID запроса: ${truncate(requestId, 64)}`);
      }
      return lines.join("\n");
    },
  });

    const recallOk = registerVariants(["memory.recall", "memory.user.recall"], {
    getQueryPreview({ args, output }) {
      const query = normalizeText(args?.query || output?.query || "");
      const key = normalizeText(args?.key || output?.key || "");
      if (query) {
        return `Память: ${truncate(query, 56)}`;
      }
      if (key) {
        return `Память key=${truncate(key, 56)}`;
      }
      return "Чтение памяти";
    },
    formatStart({ args }) {
      const query = normalizeText(args?.query || "");
      const key = normalizeText(args?.key || "");
      if (query) {
        return `**Ищу в памяти:** ${truncate(query, 220)}`;
      }
      if (key) {
        return `**Ищу в памяти по ключу:** ${truncate(key, 120)}`;
      }
      return "_Читаю память пользователя..._";
    },
    formatOutput({ output }) {
      if (!output || typeof output !== "object") {
        return "";
      }
      if (output.error) {
        return `**Ошибка чтения памяти:** ${truncate(output.error, 400)}`;
      }

      const memories = safeList(output.memories);
      if (!memories.length) {
        return "**Память:** подходящих записей не найдено.";
      }

      const lines = ["**Найдено в памяти**", ""];
      memories.slice(0, 12).forEach((entry, index) => {
        lines.push(formatMemoryLine(entry, index));
      });
      if (typeof output.count === "number" && output.count > memories.length) {
        lines.push("");
        lines.push(`И ещё: ${output.count - memories.length}`);
      }
      return lines.join("\n");
    },
  });

    const forgetOk = registerVariants(["memory.forget", "memory.user.forget"], {
    getQueryPreview({ args, output }) {
      const key = normalizeText(args?.key || "");
      const id = normalizeText(args?.id || "");
      if (id) {
        return `Удалить id=${truncate(id, 24)}`;
      }
      if (key) {
        return `Удалить key=${truncate(key, 40)}`;
      }
      const query = normalizeText(args?.query || "");
      if (query) {
        return `Забыть: ${truncate(query, 48)}`;
      }
      const removedCount = Number(output?.removed_count || 0);
      return removedCount > 0 ? `Удалено: ${removedCount}` : "Удаление памяти";
    },
    formatStart({ args }) {
      const key = normalizeText(args?.key || "");
      const id = normalizeText(args?.id || "");
      const query = normalizeText(args?.query || "");
      if (id) {
        return `**Удаляю запись памяти:** ${truncate(id, 120)}`;
      }
      if (key) {
        return `**Удаляю память по ключу:** ${truncate(key, 120)}`;
      }
      if (query) {
        return `**Удаляю память по запросу:** ${truncate(query, 220)}`;
      }
      return "_Удаляю запись памяти..._";
    },
    formatOutput({ output }) {
      if (!output || typeof output !== "object") {
        return "";
      }
      if (output.error) {
        return `**Ошибка удаления памяти:** ${truncate(output.error, 400)}`;
      }

      const removedCount = Math.max(0, Number(output.removed_count) || 0);
      const remainingCount = Math.max(0, Number(output.remaining_count) || 0);
      const removed = safeList(output.removed);

      if (removedCount <= 0) {
        return "**Память:** ничего не удалено (совпадений нет).";
      }

      const lines = [
        `**Удалено записей:** ${removedCount}`,
        `**Осталось записей:** ${remainingCount}`,
      ];
      if (removed.length > 0) {
        lines.push("");
        lines.push("Удалено:");
        removed.slice(0, 8).forEach((entry, index) => {
          lines.push(formatMemoryLine(entry, index));
        });
      }
      return lines.join("\n");
    },
    });
    return rememberOk && recallOk && forgetOk;
  }

  if (registerAll()) {
    return;
  }

  let attempts = 0;
  const maxAttempts = 40;
  const intervalId = window.setInterval(() => {
    attempts += 1;
    if (registerAll() || attempts >= maxAttempts) {
      window.clearInterval(intervalId);
    }
  }, 250);
})();
