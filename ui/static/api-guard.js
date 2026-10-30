// Convert accidental HTML error pages into a readable API error object.
// This keeps older cached app.js versions from surfacing JSON parse exceptions.
(function () {
  const nativeJson = Response.prototype.json;
  Response.prototype.json = async function () {
    const copy = this.clone();
    try {
      return await nativeJson.call(this);
    } catch (error) {
      const text = (await copy.text()).trim();
      if (text.startsWith("<")) {
        return { error: "伺服器回傳了 HTML 錯誤頁，請重新整理後再試" };
      }
      throw error;
    }
  };

  const nativeFetch = window.fetch.bind(window);
  window.fetch = async function (...args) {
    const response = await nativeFetch(...args);
    const requestUrl = typeof args[0] === "string" ? args[0] : args[0]?.url || "";
    if (!response.ok || !response.body || !/\/api\/datasets\/[^/]+\/download$/.test(requestUrl)) {
      return response;
    }
    const total = Number(response.headers.get("content-length")) || 0;
    const reader = response.body.getReader();
    let received = 0;
    const stream = new ReadableStream({
      async pull(controller) {
        const part = await reader.read();
        if (part.done) {
          controller.close();
          return;
        }
        received += part.value.byteLength;
        const panel = document.getElementById("progress-panel");
        const label = document.getElementById("progress-label");
        const value = document.getElementById("progress-value");
        const bar = document.getElementById("progress-bar");
        const detail = document.getElementById("progress-detail");
        if (panel && label && value && bar && detail) {
          panel.hidden = false;
          label.textContent = "下載檔案中";
          const percent = total ? Math.min(100, Math.round(received / total * 100)) : 0;
          value.textContent = total ? `${percent}%` : "處理中";
          bar.style.width = total ? `${percent}%` : "100%";
          detail.textContent = total
            ? `已下載 ${formatBytes(received)} / ${formatBytes(total)}`
            : `已下載 ${formatBytes(received)}`;
        }
        controller.enqueue(part.value);
      },
      cancel(reason) {
        return reader.cancel(reason);
      },
    });
    return new Response(stream, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  };
})();
