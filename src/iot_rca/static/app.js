const SVG_NS = "http://www.w3.org/2000/svg";

let latestSnapshot = null;
let selectedBlastTargetId = null;
let blastAffectedIds = new Set();

function el(tag, attrs = {}, text = null) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    node.setAttribute(k, v);
  }
  if (text !== null) node.textContent = text;
  return node;
}

function svgEl(tag, attrs = {}, text = null) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) {
    node.setAttribute(k, String(v));
  }
  if (text !== null) node.textContent = text;
  return node;
}

function computeLayout(snapshot) {
  const columns = {
    PowerPanel: { x: 40, w: 175, h: 48 },
    NetworkSwitch: { x: 270, w: 175, h: 48 },
    Gateway: { x: 510, w: 185, h: 46 },
    Sensor: { x: 770, w: 310, h: 20 },
  };

  const byType = {
    PowerPanel: [],
    NetworkSwitch: [],
    Gateway: [],
    Sensor: [],
  };
  for (const n of snapshot.nodes) {
    if (byType[n.node_type]) byType[n.node_type].push(n);
  }

  // Group sensors under their connected gateway for clean non-crossing layout
  const sensorToGateway = new Map();
  for (const edge of snapshot.edges) {
    if (edge.relationship === "CONNECTED_TO") {
      sensorToGateway.set(edge.source, edge.target);
    }
  }

  const positions = new Map();
  const totalHeight = 720;

  const placeColumn = (nodes, col, fixedGap = null) => {
    const count = nodes.length;
    if (count === 0) return;
    const step = fixedGap || Math.floor((totalHeight - 70) / count);
    const startY = 58 + Math.max(0, Math.floor((totalHeight - 70 - step * count) / 2));
    nodes.forEach((n, idx) => {
      positions.set(n.id, {
        x: col.x,
        y: startY + idx * step,
        w: col.w,
        h: col.h,
      });
    });
  };

  placeColumn(byType.PowerPanel, columns.PowerPanel);
  placeColumn(byType.NetworkSwitch, columns.NetworkSwitch);
  placeColumn(byType.Gateway, columns.Gateway);

  // Order sensors by gateway vertical order so lines stay parallel and legible
  const gwOrder = new Map(byType.Gateway.map((g, i) => [g.id, i]));
  byType.Sensor.sort((a, b) => {
    const ga = gwOrder.get(sensorToGateway.get(a.id)) ?? 99;
    const gb = gwOrder.get(sensorToGateway.get(b.id)) ?? 99;
    return ga !== gb ? ga - gb : a.id.localeCompare(b.id);
  });

  // Compact 2-column grid for sensors on the right
  const sensorCount = byType.Sensor.length;
  const rows = Math.ceil(sensorCount / 2);
  const rowStep = Math.max(26, Math.floor((totalHeight - 70) / Math.max(1, rows)));
  byType.Sensor.forEach((s, idx) => {
    const colIdx = idx % 2;
    const rowIdx = Math.floor(idx / 2);
    positions.set(s.id, {
      x: 760 + colIdx * 172,
      y: 52 + rowIdx * rowStep,
      w: 158,
      h: 22,
    });
  });

  return positions;
}

function renderTopologyGraph(snapshot) {
  const svg = document.getElementById("topology-svg");
  svg.replaceChildren();

  const sensorCount = snapshot.nodes.filter((n) => n.node_type === "Sensor").length;
  const headers = [
    { x: 40, label: "Power Panels (Passive)" },
    { x: 270, label: "Network Switches" },
    { x: 510, label: "IoT Gateways" },
    { x: 760, label: `Field Sensors (${sensorCount})` },
  ];
  for (const h of headers) {
    svg.appendChild(svgEl("text", { x: h.x, y: 26, class: "col-header" }, h.label));
  }

  const positions = computeLayout(snapshot);
  const nodeById = new Map(snapshot.nodes.map((n) => [n.id, n]));
  const rootCauseIds = new Set(snapshot.open_incidents.map((inc) => inc.root_cause_id));

  // Edges layer (draw target -> source so power flows left -> right visually)
  for (const edge of snapshot.edges) {
    const srcPos = positions.get(edge.source);
    const tgtPos = positions.get(edge.target);
    if (!srcPos || !tgtPos) continue;

    const x1 = tgtPos.x + tgtPos.w;
    const y1 = tgtPos.y + tgtPos.h / 2;
    const x2 = srcPos.x;
    const y2 = srcPos.y + srcPos.h / 2;
    const midX = (x1 + x2) / 2;

    const srcNode = nodeById.get(edge.source);
    const tgtNode = nodeById.get(edge.target);
    const isBroken = srcNode?.status === "offline" || tgtNode?.status === "offline";
    const isBlast =
      selectedBlastTargetId &&
      (edge.target === selectedBlastTargetId || blastAffectedIds.has(edge.target)) &&
      blastAffectedIds.has(edge.source);

    let cls = "edge-line";
    if (isBlast) cls += " blast";
    else if (isBroken) cls += " broken";

    svg.appendChild(
      svgEl("path", {
        d: `M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`,
        class: cls,
      })
    );
  }

  // Nodes layer
  for (const node of snapshot.nodes) {
    const pos = positions.get(node.id);
    if (!pos) continue;

    const g = svgEl("g", { class: "node-group" });
    g.addEventListener("click", () => inspectBlastRadius(node.id));

    let rectCls = `node-rect ${node.status}`;
    if (rootCauseIds.has(node.id)) rectCls += " root-cause";
    else if (node.id === selectedBlastTargetId || blastAffectedIds.has(node.id)) {
      rectCls += " blast";
    }

    g.appendChild(
      svgEl("rect", {
        x: pos.x,
        y: pos.y,
        width: pos.w,
        height: pos.h,
        class: rectCls,
      })
    );

    if (node.node_type === "Sensor") {
      g.appendChild(
        svgEl("text", { x: pos.x + 8, y: pos.y + 14.5, class: "node-label" }, node.id)
      );
      const badge = node.status === "online" ? "OK" : node.status === "offline" ? "OFF" : "?";
      g.appendChild(
        svgEl("text", { x: pos.x + pos.w - 30, y: pos.y + 14, class: "node-sub" }, badge)
      );
    } else {
      const title = rootCauseIds.has(node.id) ? `${node.id} [ROOT CAUSE]` : node.id;
      g.appendChild(
        svgEl("text", { x: pos.x + 10, y: pos.y + 19, class: "node-label" }, title)
      );
      const subText = node.is_passive ? "passive panel" : `${node.node_type} · ${node.status}`;
      g.appendChild(
        svgEl("text", { x: pos.x + 10, y: pos.y + 35, class: "node-sub" }, subText)
      );
    }

    svg.appendChild(g);
  }
}

function renderKpis(snapshot) {
  const stats = snapshot.stats;
  const activeTotal = stats.active_devices_online + stats.active_devices_offline;
  document.getElementById("kpi-online").textContent = `${stats.active_devices_online} / ${activeTotal}`;
  document.getElementById("kpi-sensors").textContent =
    `Sensors online: ${stats.sensors_online} / ${stats.sensors_total}`;
  document.getElementById("kpi-incidents").textContent = String(stats.open_incidents_count);
  document.getElementById("kpi-raw").textContent =
    `Equivalent raw alerts: ${stats.raw_alerts_equivalent}`;
  document.getElementById("kpi-suppressed").textContent = String(stats.suppressed_alerts);
  document.getElementById("kpi-noise").textContent = `${stats.noise_reduction_pct}%`;
  document.getElementById("kpi-scenario").textContent = `Scenario: ${snapshot.active_scenario}`;

  document.querySelectorAll(".chaos-controls button[data-scenario]").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.scenario === snapshot.active_scenario);
  });
}

function renderIncidents(incidents) {
  const container = document.getElementById("incidents-list");
  container.replaceChildren();
  if (!incidents.length) {
    container.className = "scroll-list empty-state";
    container.textContent = "No open incidents — fleet healthy.";
    return;
  }

  container.className = "scroll-list";
  for (const inc of incidents) {
    const card = el("div", { class: "incident-card" });
    const top = el("div", { class: "incident-top" });
    top.appendChild(el("span", {}, `${inc.id} · ${inc.root_cause_id}`));
    top.appendChild(el("span", { class: `badge ${inc.severity}` }, inc.severity));

    const summary = el("div", {}, inc.summary);
    const meta = el(
      "div",
      { class: "kpi-sub" },
      `Type: ${inc.root_cause_type} | Raw alerts: ${inc.raw_alerts_equivalent} | Suppressed: ${inc.suppressed_alerts_count}`
    );

    card.appendChild(top);
    card.appendChild(summary);
    card.appendChild(meta);
    container.appendChild(card);
  }
}

async function refreshStreamEvents() {
  try {
    const res = await fetch("/api/v1/events/stream?count=20");
    if (!res.ok) return;
    const events = await res.json();
    const container = document.getElementById("stream-list");
    container.replaceChildren();
    if (!events.length) {
      container.textContent = "Waiting for device events...";
      return;
    }
    for (const ev of events) {
      const row = el("div", { class: "stream-item" });
      const evCls = ev.event === "ONLINE" ? "ev-online" : "ev-offline";
      row.appendChild(el("span", {}, `${ev.device_id} (${ev.device_type})`));
      row.appendChild(el("span", { class: evCls }, ev.event));
      container.appendChild(row);
    }
  } catch (err) {
    console.warn("Stream refresh failed", err);
  }
}

async function inspectBlastRadius(nodeId) {
  selectedBlastTargetId = nodeId;
  const panel = document.getElementById("blast-panel");
  try {
    const res = await fetch(`/api/v1/blast-radius/${encodeURIComponent(nodeId)}`);
    if (!res.ok) return;
    const report = await res.json();
    blastAffectedIds = new Set(report.affected_nodes.map((n) => n.id));

    panel.replaceChildren();
    panel.className = "blast-box";
    panel.appendChild(
      el(
        "div",
        {},
        `${report.target_id} (${report.target_type}) -> ${report.affected_sensor_count} sensors, ${report.affected_infra_count} infra nodes impacted`
      )
    );
    const cb = report.criticality_breakdown;
    panel.appendChild(
      el(
        "div",
        { class: "kpi-sub" },
        `Critical: ${cb.critical || 0} | High: ${cb.high || 0} | Medium: ${cb.medium || 0} | Low: ${cb.low || 0}`
      )
    );
    const tags = el("div", { class: "blast-tags" });
    for (const item of report.affected_nodes) {
      tags.appendChild(el("span", { class: "blast-tag" }, `${item.id} (${item.hops}h)`));
    }
    panel.appendChild(tags);

    renderNodeDetails(panel, nodeId);
    if (latestSnapshot) renderTopologyGraph(latestSnapshot);
  } catch (err) {
    console.error("Blast radius query failed", err);
  }
}

async function setChaosScenario(scenario) {
  await fetch("/api/v1/chaos", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scenario }),
  });
}

function renderNodeDetails(panel, nodeId) {
  if (!latestSnapshot || !Array.isArray(latestSnapshot.nodes)) return;
  const node = latestSnapshot.nodes.find((n) => n.id === nodeId);
  if (!node) return;

  const box = document.createElement("div");
  box.className = "node-details";

  const head = document.createElement("div");
  head.className = "node-details-head";
  const title = document.createElement("span");
  title.textContent = node.zone_id ? `${node.node_type} · ${node.zone_id}` : node.node_type;
  const pill = document.createElement("span");
  pill.className = `status-pill ${node.status}`;
  pill.textContent = node.status;
  head.append(title, pill);
  box.appendChild(head);

  const meta = document.createElement("div");
  meta.className = "node-details-meta";
  const parts = [];
  if (node.criticality) parts.push(`Criticality: ${node.criticality}`);
  if (node.is_passive) parts.push("Passive infrastructure");
  if (node.battery_pct !== null && node.battery_pct !== undefined) {
    parts.push(`Battery: ${node.battery_pct}%`);
  }
  if (node.last_seen) {
    parts.push(`Last seen: ${new Date(node.last_seen).toLocaleTimeString()}`);
  }
  if (parts.length > 0) {
    meta.textContent = parts.join(" | ");
    box.appendChild(meta);
  }

  const metricEntries = node.metrics ? Object.entries(node.metrics) : [];
  if (metricEntries.length > 0) {
    const metricsRow = document.createElement("div");
    metricsRow.className = "node-details-metrics";
    metricsRow.textContent = `Telemetry: ${metricEntries.map(([k, v]) => `${k}=${v}`).join(", ")}`;
    box.appendChild(metricsRow);
  }

  panel.appendChild(box);
}

async function refreshResolvedIncidents() {
  const list = document.getElementById("resolved-list");
  if (!list) return;
  try {
    const res = await fetch("/api/v1/incidents?status=RESOLVED&limit=8");
    const items = await res.json();
    list.replaceChildren();
    if (!items.length) {
      list.className = "scroll-list empty-state";
      list.textContent = "No resolved incidents yet.";
      return;
    }
    list.className = "scroll-list";
    for (const inc of items) {
      const card = document.createElement("div");
      card.className = "history-card";

      const top = document.createElement("div");
      top.className = "incident-top";
      const title = document.createElement("span");
      title.textContent = `${inc.root_cause_name} (${inc.root_cause_type})`;
      const badge = document.createElement("span");
      badge.className = "badge low";
      badge.textContent = "RESOLVED";
      top.append(title, badge);

      const summary = document.createElement("div");
      summary.textContent = inc.summary;

      const meta = document.createElement("div");
      meta.className = "mono-tag";
      const resolvedAt = inc.resolved_at ? new Date(inc.resolved_at).toLocaleTimeString() : "-";
      meta.textContent = `Resolved: ${resolvedAt} | Suppressed: ${inc.suppressed_alerts_count}`;

      card.append(top, summary, meta);
      list.appendChild(card);
    }
  } catch (err) {
    console.error("Failed to fetch resolved incidents", err);
  }
}

function applySnapshot(snapshot) {
  latestSnapshot = snapshot;
  renderKpis(snapshot);
  renderIncidents(snapshot.open_incidents);
  renderTopologyGraph(snapshot);
  refreshStreamEvents();
  refreshResolvedIncidents();
}

function connectWebSocket() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws/live`);
  const badge = document.getElementById("ws-status");
  const statusText = document.getElementById("ws-status-text");

  ws.onopen = () => {
    badge.classList.add("connected");
    statusText.textContent = "Live Stream Connected";
  };
  ws.onmessage = (event) => {
    applySnapshot(JSON.parse(event.data));
  };
  ws.onclose = () => {
    badge.classList.remove("connected");
    statusText.textContent = "Reconnecting...";
    setTimeout(connectWebSocket, 2000);
  };
}

document.querySelectorAll(".chaos-controls button[data-scenario]").forEach((btn) => {
  btn.addEventListener("click", () => setChaosScenario(btn.dataset.scenario));
});

document.getElementById("btn-evaluate").addEventListener("click", async () => {
  await fetch("/api/v1/rca/evaluate", { method: "POST" });
});

document.getElementById("btn-clear-blast").addEventListener("click", () => {
  selectedBlastTargetId = null;
  blastAffectedIds = new Set();
  const panel = document.getElementById("blast-panel");
  panel.className = "blast-box empty-state";
  panel.textContent = "Click any PowerPanel, Switch, or Gateway node to trace downstream impact.";
  if (latestSnapshot) renderTopologyGraph(latestSnapshot);
});

fetch("/api/v1/topology")
  .then((r) => r.json())
  .then(applySnapshot)
  .catch(() => {});
connectWebSocket();
