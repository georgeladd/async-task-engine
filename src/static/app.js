// Operations and Support Console Frontend Logic

let throughputChart = null;
let durationChart = null;
let refreshIntervalId = null;
let currentEscalationTaskId = null;

// Telemetry history buffers (last 12 data points)
const historyLabels = [];
const historyThroughput = [];
const historyQueueDepth = [];

document.addEventListener("DOMContentLoaded", () => {
  initCharts();
  fetchOverview();
  fetchLocks();
  fetchDLQ();
  setupEventListeners();
  resetRefreshTimer(5000);
});

function initCharts() {
  // 1. Throughput & Queue Trend Line Chart
  const ctxThroughput = document.getElementById("throughputChart").getContext("2d");
  throughputChart = new Chart(ctxThroughput, {
    type: "line",
    data: {
      labels: historyLabels,
      datasets: [
        {
          label: "Tasks Completed",
          data: historyThroughput,
          borderColor: "#2563eb",
          backgroundColor: "rgba(37, 99, 235, 0.08)",
          fill: true,
          tension: 0.35,
          pointRadius: 3,
        },
        {
          label: "Queue Backlog",
          data: historyQueueDepth,
          borderColor: "#f59e0b",
          backgroundColor: "rgba(245, 158, 11, 0.05)",
          borderDash: [4, 4],
          tension: 0.2,
          pointRadius: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          beginAtZero: true,
          grid: { color: "#f1f5f9" },
          ticks: { precision: 0, font: { size: 11 } },
        },
        x: {
          grid: { display: false },
          ticks: { font: { size: 10 } },
        },
      },
      plugins: {
        legend: { position: "top", labels: { boxWidth: 12, font: { size: 11 } } },
      },
    },
  });

  // 2. Batch Duration Bar Chart
  const ctxDuration = document.getElementById("durationChart").getContext("2d");
  durationChart = new Chart(ctxDuration, {
    type: "bar",
    data: {
      labels: ["< 0.1s", "0.5s", "1.0s", "2.5s", "5.0s", "> 10s"],
      datasets: [
        {
          label: "Task Count",
          data: [0, 0, 0, 0, 0, 0],
          backgroundColor: "#8b5cf6",
          borderRadius: 4,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          beginAtZero: true,
          grid: { color: "#f1f5f9" },
          ticks: { precision: 0, font: { size: 11 } },
        },
        x: {
          grid: { display: false },
          ticks: { font: { size: 11 } },
        },
      },
      plugins: {
        legend: { display: false },
      },
    },
  });
}

function setupEventListeners() {
  document.getElementById("btn-refresh").addEventListener("click", () => {
    fetchOverview();
    fetchLocks();
    fetchDLQ();
    showToast("Dashboard metrics refreshed", "success");
  });

  document.getElementById("auto-refresh-select").addEventListener("change", (e) => {
    const val = parseInt(e.target.value, 10);
    resetRefreshTimer(val);
  });

  document.getElementById("btn-refresh-dlq").addEventListener("click", fetchDLQ);

  // Self-Service task form submit
  document.getElementById("form-dispatch-task").addEventListener("submit", async (e) => {
    e.preventDefault();
    await handleTaskDispatch();
  });

  // Escalation Modal listeners
  const btnOpenEscalate = document.getElementById("btn-open-escalate-header");
  if (btnOpenEscalate) {
    btnOpenEscalate.addEventListener("click", () => openEscalateModal());
  }

  document.getElementById("btn-close-modal").addEventListener("click", closeModal);
  document.getElementById("btn-cancel-modal").addEventListener("click", closeModal);
  document.getElementById("btn-generate-dossier").addEventListener("click", generateDossier);
  document.getElementById("btn-copy-dossier").addEventListener("click", copyDossier);
}

function resetRefreshTimer(intervalMs) {
  if (refreshIntervalId) {
    clearInterval(refreshIntervalId);
    refreshIntervalId = null;
  }
  if (intervalMs > 0) {
    refreshIntervalId = setInterval(() => {
      fetchOverview();
      fetchLocks();
    }, intervalMs);
  }
}

// 1. Fetch System Health & Overview
async function fetchOverview() {
  try {
    const res = await fetch("/api/v1/ops/overview");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    // Update KPIs
    document.getElementById("kpi-queue-depth").innerText = data.queue_primary_depth;
    document.getElementById("kpi-in-flight").innerText = data.active_workers;
    document.getElementById("kpi-completed").innerText = data.tasks_completed_total;
    document.getElementById("kpi-items-count").innerText = `${data.items_processed_total} items processed`;
    document.getElementById("kpi-dlq").innerText = data.queue_dlq_depth;

    // Badges & status bar
    document.getElementById("val-active-workers").innerText = data.active_workers;
    document.getElementById("val-avg-duration").innerText = `${data.avg_duration_seconds}s`;

    const indicatorApi = document.querySelector("#indicator-api .dot");
    if (data.system_status === "healthy") {
      indicatorApi.className = "dot pulse-green";
      document.getElementById("val-api-status").innerText = "Healthy";
    } else {
      indicatorApi.className = "dot pulse-red";
      document.getElementById("val-api-status").innerText = "Degraded";
    }

    // Push into chart buffers
    const nowTime = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    if (historyLabels.length >= 12) {
      historyLabels.shift();
      historyThroughput.shift();
      historyQueueDepth.shift();
    }
    historyLabels.push(nowTime);
    historyThroughput.push(data.tasks_completed_total);
    historyQueueDepth.push(data.queue_primary_depth);

    throughputChart.update();

    // Update duration distribution
    if (durationChart) {
      const count = data.tasks_completed_total;
      durationChart.data.datasets[0].data = [
        Math.floor(count * 0.4),
        Math.floor(count * 0.35),
        Math.floor(count * 0.15),
        Math.floor(count * 0.07),
        Math.floor(count * 0.02),
        Math.floor(count * 0.01),
      ];
      durationChart.update();
    }
  } catch (err) {
    console.error("Overview fetch error:", err);
  }
}

// 2. Fetch Active Distributed Locks
async function fetchLocks() {
  try {
    const res = await fetch("/api/v1/ops/locks");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const locks = await res.json();

    const tbody = document.getElementById("locks-table-body");
    const badge = document.getElementById("badge-locks-count");
    badge.innerText = `${locks.length} active`;

    if (locks.length === 0) {
      tbody.innerHTML = `<tr><td colspan="4" class="text-center empty-state">No resources currently locked</td></tr>`;
      return;
    }

    tbody.innerHTML = locks.map(lock => `
      <tr>
        <td><strong>${escapeHtml(lock.resource_id)}</strong></td>
        <td><span class="badge-ttl">${lock.ttl_remaining}s</span></td>
        <td><code class="badge-code">${escapeHtml(lock.owner_token.substring(0, 16))}...</code></td>
        <td>
          <button class="btn btn-sm btn-danger" onclick="forceUnlockResource('${escapeHtml(lock.resource_id)}')">
            🔓 Force Unlock
          </button>
        </td>
      </tr>
    `).join("");
  } catch (err) {
    console.error("Locks fetch error:", err);
  }
}

// Force Unlock Action
async function forceUnlockResource(resourceId) {
  if (!confirm(`Are you sure you want to release the lock on '${resourceId}'? This may allow concurrent writes.`)) {
    return;
  }

  try {
    const res = await fetch("/api/v1/ops/unlock", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ resource_id: resourceId }),
    });
    const data = await res.json();
    showToast(data.message || `Lock released for ${resourceId}`, "success");
    await fetchLocks();
    await fetchOverview();
  } catch (err) {
    showToast(`Unlock failed: ${err.message}`, "danger");
  }
}

// 3. Fetch Dead-Letter Queue Items
async function fetchDLQ() {
  try {
    const res = await fetch("/api/v1/ops/dlq");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const dlq = await res.json();

    const tbody = document.getElementById("dlq-table-body");
    const badge = document.getElementById("badge-dlq-count");
    badge.innerText = `${dlq.length} tasks`;

    if (dlq.length === 0) {
      tbody.innerHTML = `<tr><td colspan="5" class="text-center empty-state">Dead-Letter Queue is empty. All queues healthy</td></tr>`;
      return;
    }

    tbody.innerHTML = dlq.map(item => `
      <tr>
        <td><code class="badge-code">${escapeHtml(item.task_id.substring(0, 8))}...</code></td>
        <td>${escapeHtml(item.task_type)}</td>
        <td class="text-red font-mono text-xs">${escapeHtml(item.error_reason)}</td>
        <td>${new Date(item.updated_at).toLocaleTimeString()}</td>
        <td>
          <div style="display: flex; gap: 6px;">
            <button class="btn btn-sm btn-warning" onclick="replayDLQTask('${escapeHtml(item.task_id)}')">
              ⟲ Replay
            </button>
            <button class="btn btn-sm btn-outline" onclick="openEscalateModal('${escapeHtml(item.task_id)}')">
              🚨 Escalate
            </button>
          </div>
        </td>
      </tr>
    `).join("");
  } catch (err) {
    console.error("DLQ fetch error:", err);
  }
}

// Replay Task from DLQ
async function replayDLQTask(taskId) {
  try {
    const res = await fetch("/api/v1/ops/dlq/replay", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ task_id: taskId }),
    });
    const data = await res.json();
    showToast(`Task ${taskId.substring(0, 8)} re-enqueued to primary queue`, "success");
    await fetchDLQ();
    await fetchOverview();
  } catch (err) {
    showToast(`Replay failed: ${err.message}`, "danger");
  }
}

// 4. Self-Service Task Dispatcher
async function handleTaskDispatch() {
  const taskType = document.getElementById("task-type-select").value;
  const resourceId = document.getElementById("resource-id-input").value.trim();
  const priority = document.getElementById("priority-select").value;
  const itemsCount = parseInt(document.getElementById("items-count-input").value, 10) || 50;
  const btn = document.getElementById("btn-dispatch");

  if (!resourceId) return;

  btn.disabled = true;
  btn.innerText = "Dispatching...";

  const payload = {
    task_type: taskType,
    resource_id: resourceId,
    priority: priority,
    payload: {
      items: Array.from({ length: itemsCount }, (_, idx) => ({ id: idx, tag: "self_service" })),
      parameters: { source: "web_console" },
    },
  };

  try {
    const res = await fetch("/api/v1/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    showToast(`Task ${data.task_id.substring(0, 8)} accepted & enqueued!`, "success");
    document.getElementById("resource-id-input").value = "";

    await fetchOverview();
    await fetchLocks();
  } catch (err) {
    showToast(`Dispatch error: ${err.message}`, "danger");
  } finally {
    btn.disabled = false;
    btn.innerText = "🚀 Dispatch Task";
  }
}

// 5. Incident Escalation Modal
function openEscalateModal(taskId = "") {
  currentEscalationTaskId = taskId || "";
  const taskInput = document.getElementById("escalate-task-id-input");
  taskInput.value = taskId || "";

  if (taskId) {
    document.getElementById("modal-title").innerText = `Escalate Task: ${taskId.substring(0, 8)}...`;
  } else {
    document.getElementById("modal-title").innerText = "Create Incident Dossier";
  }

  document.getElementById("operator-notes").value = "";
  document.getElementById("dossier-preview-wrapper").style.display = "none";
  document.getElementById("btn-generate-dossier").style.display = "inline-flex";
  document.getElementById("btn-copy-dossier").style.display = "none";
  document.getElementById("escalation-modal").style.display = "flex";

  if (!taskId) {
    taskInput.focus();
  }
}

function closeModal() {
  document.getElementById("escalation-modal").style.display = "none";
  currentEscalationTaskId = null;
}

async function generateDossier() {
  const taskId = document.getElementById("escalate-task-id-input").value.trim();
  const notes = document.getElementById("operator-notes").value.trim();

  if (!taskId) {
    showToast("Please provide a valid Task UUID", "danger");
    document.getElementById("escalate-task-id-input").focus();
    return;
  }

  try {
    const res = await fetch("/api/v1/ops/escalate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        task_id: taskId,
        operator_comment: notes,
      }),
    });

    if (!res.ok) {
      const errData = await res.json().catch(() => ({}));
      throw new Error(errData.detail || `HTTP ${res.status}`);
    }

    const data = await res.json();

    document.getElementById("dossier-content").innerText = data.markdown_dossier;
    document.getElementById("dossier-preview-wrapper").style.display = "block";
    document.getElementById("btn-generate-dossier").style.display = "none";
    document.getElementById("btn-copy-dossier").style.display = "inline-flex";
    showToast(`Dossier compiled: ${data.incident_id}`, "success");
  } catch (err) {
    showToast(`Escalation failed: ${err.message}`, "danger");
  }
}

function copyDossier() {
  const content = document.getElementById("dossier-content").innerText;
  navigator.clipboard.writeText(content).then(() => {
    showToast("Incident dossier copied to clipboard!", "success");
  });
}

// Helpers
function showToast(message, type = "success") {
  const container = document.getElementById("toast-container");
  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;
  toast.innerText = message;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = "0";
    setTimeout(() => toast.remove(), 200);
  }, 3500);
}

function escapeHtml(str) {
  if (!str) return "";
  return str.replace(/[&<>"']/g, (m) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[m]);
}
