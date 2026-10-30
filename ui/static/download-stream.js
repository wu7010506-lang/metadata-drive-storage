// Prepare verified downloads as background jobs, then hand the finished file
// to the browser's native download manager. No large response is read by fetch.
(function () {
  const rows = document.getElementById("dataset-rows");
  if (!rows) return;

  function setProgress(status, completed, total) {
    const panel = document.getElementById("progress-panel");
    const label = document.getElementById("progress-label");
    const value = document.getElementById("progress-value");
    const bar = document.getElementById("progress-bar");
    const detail = document.getElementById("progress-detail");
    if (!panel || !label || !value || !bar || !detail) return;
    panel.hidden = false;
    const percent = total ? Math.min(100, Math.round(completed / total * 100)) : 0;
    label.textContent = status === "complete" ? "驗證完成，開始下載" : "讀取並驗證 Drive chunks";
    value.textContent = `${percent}%`;
    bar.style.width = `${percent}%`;
    detail.textContent = total
      ? `已驗證 ${completed} / ${total} 個 chunks`
      : "正在準備下載工作…";
  }

  async function readError(response, fallback) {
    try {
      const data = await response.json();
      return data.error || fallback;
    } catch (error) {
      return fallback;
    }
  }

  async function pollDownload(jobId) {
    while (true) {
      const response = await fetch(`/api/downloads/${encodeURIComponent(jobId)}`);
      if (!response.ok) throw new Error(await readError(response, "無法讀取下載進度"));
      const job = await response.json();
      setProgress(job.status, job.completedChunks || 0, job.totalChunks || 0);
      if (job.status === "complete") return job;
      if (job.status === "error") throw new Error(job.error || "下載準備失敗");
      await new Promise((resolve) => setTimeout(resolve, 600));
    }
  }

  async function download(button) {
    const datasetId = button.dataset.datasetId;
    const original = button.textContent;
    button.disabled = true;
    button.textContent = "準備中…";
    try {
      const startedResponse = await fetch(
        `/api/datasets/${encodeURIComponent(datasetId)}/downloads`,
        { method: "POST" },
      );
      if (!startedResponse.ok) {
        throw new Error(await readError(startedResponse, "無法開始下載工作"));
      }
      const started = await startedResponse.json();
      const job = await pollDownload(started.jobId);
      const result = document.getElementById("result");
      if (result) {
        result.className = "result success";
        result.textContent = `檔案已驗證並儲存至：${job.savedPath}`;
        result.hidden = false;
      }
      button.textContent = "已儲存";
    } catch (error) {
      button.textContent = "失敗";
      alert(error?.message || "下載失敗，請稍後再試");
    } finally {
      setTimeout(() => {
        button.textContent = original;
        button.disabled = false;
      }, 1500);
    }
  }

  rows.addEventListener("click", (event) => {
    const button = event.target.closest("[data-dataset-id]");
    if (!button) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    download(button);
  }, true);
})();
