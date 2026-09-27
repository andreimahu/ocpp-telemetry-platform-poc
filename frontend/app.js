const stationIds = ["station-001", "station-002"];
const connectorIds = [1, 2];
const state = {
  chargers: new Map(),
  pending: new Set(),
  demoPending: new Set(),
  demoResults: new Map(),
};

const stationsElement = document.querySelector("#stations");
const lastSyncElement = document.querySelector("#last-sync");
const indicatorElement = document.querySelector("#api-indicator");
const errorElement = document.querySelector("#error-banner");

function connectorKey(chargerId, connectorId) {
  return `${chargerId}:${connectorId}`;
}

function connectorState(chargerId, connectorId) {
  const charger = state.chargers.get(chargerId);
  return charger?.connectors.find((connector) => connector.connectorId === connectorId);
}

function statusClass(status) {
  if (status === "Charging") return "charging";
  if (status === "Preparing" || status === "Finishing") return "transitioning";
  if (status === "Faulted" || status === "Unavailable") return "error";
  return "";
}

function shortId(eventId) {
  return eventId ? `${eventId.slice(0, 8)}…` : "—";
}

function formatTime(value) {
  if (!value) return "—";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function renderDemoResult(result) {
  if (!result) return "";
  if (result.type === "pending") {
    return `<div class="demo-result pending">Waiting for the processor and read model…</div>`;
  }
  if (result.type === "error") {
    return `<div class="demo-result failed">${result.message}</div>`;
  }
  if (result.type === "duplicate") {
    return `
      <div class="demo-result ${result.passed ? "passed" : "failed"}">
        <strong>${result.passed ? "Duplicate handled" : "Unexpected result"}</strong>
        <div class="demo-metrics">
          <span>Published <b>${result.publishedCopies}</b></span>
          <span>History rows <b>${result.storedRows}</b></span>
        </div>
        <small>Event ${shortId(result.eventId)} was sent twice with the same event ID.</small>
      </div>`;
  }

  return `
    <div class="demo-result ${result.passed ? "passed" : "failed"}">
      <strong>${result.passed ? "Late event handled" : "Unexpected result"}</strong>
      <div class="demo-event">
        <span>1</span>
        <div><b>${result.newer.status}</b><small>Reported ${formatTime(result.newer.reportedAt)}</small></div>
        <em>Latest</em>
      </div>
      <div class="demo-event">
        <span>2</span>
        <div><b>${result.late.status}</b><small>Reported ${formatTime(result.late.reportedAt)}</small></div>
        <em>History only</em>
      </div>
      <small>The older event was processed second but did not replace the latest state.</small>
    </div>`;
}

function stationCard(chargerId, index) {
  const charger = state.chargers.get(chargerId);
  const stationStatus = charger?.stationStatus?.status ?? "Connecting";
  const connectors = connectorIds.map((connectorId) => {
    const key = connectorKey(chargerId, connectorId);
    const connector = connectorState(chargerId, connectorId);
    const currentStatus = connector?.status ?? "Waiting for status";
    const cssStatus = statusClass(currentStatus);
    const busy = state.pending.has(key) || state.demoPending.has(key);
    const transition = currentStatus === "Preparing" || currentStatus === "Finishing";
    return `
      <article class="connector">
        <div class="connector-main">
          <div class="connector-name">
            <span class="status-dot ${cssStatus}" aria-hidden="true"></span>
            Connector ${connectorId}
          </div>
          <span class="status-label ${cssStatus}">${currentStatus}</span>
        </div>
        <div class="controls">
          <button class="primary" data-action="start" data-charger="${chargerId}" data-connector="${connectorId}"
            ${busy || currentStatus !== "Available" ? "disabled" : ""}>Start charging</button>
          <button data-action="stop" data-charger="${chargerId}" data-connector="${connectorId}"
            ${busy || currentStatus !== "Charging" ? "disabled" : ""}>Stop charging</button>
        </div>
        <div class="event-id" title="${connector?.eventId ?? "No event yet"}">
          EVENT ${connector?.eventId ?? "—"}
        </div>
        <div class="correctness-demo">
          <p>Event handling demo</p>
          <div class="demo-controls">
            <button data-demo="duplicate" data-charger="${chargerId}" data-connector="${connectorId}"
              ${busy || transition || !connector ? "disabled" : ""}>Send duplicate</button>
            <button data-demo="out-of-order" data-charger="${chargerId}" data-connector="${connectorId}"
              ${busy || transition || !connector ? "disabled" : ""}>Send late event</button>
          </div>
          ${renderDemoResult(state.demoResults.get(key))}
        </div>
      </article>`;
  }).join("");

  return `
    <section class="station">
      <header class="station-head">
        <div>
          <p class="station-number">STATION 0${index + 1}</p>
          <h3>${chargerId}</h3>
        </div>
        <span class="station-status">${stationStatus}</span>
      </header>
      ${connectors}
    </section>`;
}

function render() {
  stationsElement.innerHTML = stationIds.map(stationCard).join("");
}

async function pollStatus() {
  try {
    const response = await fetch("/api/status/chargers", { cache: "no-store" });
    if (!response.ok) throw new Error(`Read API returned ${response.status}`);
    const payload = await response.json();
    state.chargers = new Map(
      payload.chargers.map((charger) => [charger.chargerId, charger])
    );
    indicatorElement.className = "pulse online";
    lastSyncElement.textContent = formatTime(new Date());
    errorElement.hidden = true;
    render();
  } catch (error) {
    indicatorElement.className = "pulse offline";
    lastSyncElement.textContent = "Read API unavailable";
    errorElement.textContent = error.message;
    errorElement.hidden = false;
  }
}

async function sendCommand(chargerId, connectorId, action) {
  const key = connectorKey(chargerId, connectorId);
  state.pending.add(key);
  errorElement.hidden = true;
  render();
  try {
    const response = await fetch(
      `/api/simulator/chargers/${encodeURIComponent(chargerId)}/connectors/${connectorId}/${action}`,
      { method: "POST" }
    );
    if (!response.ok) {
      const problem = await response.json().catch(() => ({}));
      throw new Error(problem.detail ?? `Simulator API returned ${response.status}`);
    }
    await pollStatus();
  } catch (error) {
    errorElement.textContent = error.message;
    errorElement.hidden = false;
  } finally {
    state.pending.delete(key);
    render();
  }
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function fetchDiagnostics(chargerId, connectorId) {
  const response = await fetch(
    `/api/status/chargers/${encodeURIComponent(chargerId)}/connectors/${connectorId}/events?limit=20`,
    { cache: "no-store" }
  );
  if (!response.ok) throw new Error(`History API returned ${response.status}`);
  return response.json();
}

async function waitForDiagnostics(chargerId, connectorId, predicate) {
  for (let attempt = 0; attempt < 24; attempt += 1) {
    const diagnostics = await fetchDiagnostics(chargerId, connectorId);
    if (predicate(diagnostics)) return diagnostics;
    await delay(250);
  }
  throw new Error("Timed out waiting for the read model");
}

async function runDemo(chargerId, connectorId, demo) {
  const key = connectorKey(chargerId, connectorId);
  const connector = connectorState(chargerId, connectorId);
  state.demoPending.add(key);
  state.demoResults.set(key, { type: "pending" });
  render();

  try {
    const suffix = demo === "duplicate"
      ? `duplicate?status=${encodeURIComponent(connector.status)}`
      : "out-of-order";
    const response = await fetch(
      `/api/simulator/chargers/${encodeURIComponent(chargerId)}/connectors/${connectorId}/demo/${suffix}`,
      { method: "POST" }
    );
    if (!response.ok) {
      const problem = await response.json().catch(() => ({}));
      throw new Error(problem.detail ?? `Simulator API returned ${response.status}`);
    }
    const accepted = await response.json();

    if (demo === "duplicate") {
      const diagnostics = await waitForDiagnostics(
        chargerId,
        connectorId,
        (data) => data.events.some((item) => item.eventId === accepted.eventId)
      );
      const storedRows = diagnostics.events.filter(
        (item) => item.eventId === accepted.eventId
      ).length;
      state.demoResults.set(key, {
        type: "duplicate",
        passed: storedRows === 1,
        eventId: accepted.eventId,
        publishedCopies: accepted.publishedCopies,
        storedRows,
      });
    } else {
      const expectedIds = new Set([accepted.newerEventId, accepted.lateEventId]);
      const diagnostics = await waitForDiagnostics(
        chargerId,
        connectorId,
        (data) => expectedIds.size === data.events.filter(
          (item) => expectedIds.has(item.eventId)
        ).length
      );
      const newer = diagnostics.events.find(
        (item) => item.eventId === accepted.newerEventId
      );
      const late = diagnostics.events.find(
        (item) => item.eventId === accepted.lateEventId
      );
      state.demoResults.set(key, {
        type: "out-of-order",
        passed: Boolean(newer && late && diagnostics.latest.eventId === newer.eventId),
        newer,
        late,
      });
    }
    await pollStatus();
  } catch (error) {
    state.demoResults.set(key, { type: "error", message: error.message });
  } finally {
    state.demoPending.delete(key);
    render();
  }
}

stationsElement.addEventListener("click", (event) => {
  const demoButton = event.target.closest("button[data-demo]");
  if (demoButton) {
    runDemo(
      demoButton.dataset.charger,
      Number(demoButton.dataset.connector),
      demoButton.dataset.demo
    );
    return;
  }

  const actionButton = event.target.closest("button[data-action]");
  if (!actionButton) return;
  sendCommand(
    actionButton.dataset.charger,
    Number(actionButton.dataset.connector),
    actionButton.dataset.action
  );
});

render();
pollStatus();
setInterval(pollStatus, 1000);
