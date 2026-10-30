// Display the server's non-overlapping phases; never infer pure API latency.
(function () {
  const labels = {
    receiving: "接收與暫存",
    queued: "排隊等待",
    setup: "工作初始化",
    preparing: "壓縮與 SHA-256",
    indexing: "索引與續傳檢查",
    transferring: "切塊、索引與 Drive 寫入",
  };
  const panel = document.getElementById("upload-timings");
  const values = document.getElementById("upload-timing-values");
  const summary = document.getElementById("upload-timing-summary");
  window.UploadTimingView = {
    reset() {
      if (panel) panel.hidden = true;
    },
    render(timings) {
      if (!panel || !values || !summary || !timings?.seconds) return;
      const seconds = timings.seconds;
      summary.textContent = `上傳階段耗時 · ${Number(timings.totalSeconds).toFixed(2)} 秒`;
      values.replaceChildren();
      for (const [phase, label] of Object.entries(labels)) {
        const term = document.createElement("dt");
        const value = document.createElement("dd");
        term.textContent = label;
        value.textContent = `${Number(seconds[phase] || 0).toFixed(2)} 秒${timings.phase === phase ? "（進行中）" : ""}`;
        values.append(term, value);
      }
      panel.hidden = false;
    },
  };
})();
