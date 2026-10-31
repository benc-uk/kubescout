import { severities, clusterScope, namespaces, selectIssues, paginate, severityCounts, validateSnapshot, validateDetail } from "./model.js";

const $ = (id) => document.getElementById(id);
let snapshot = null;
let stale = false;
let page = 1;
let timer;
let refreshing = false;
let listRequest;
let selected = null;
let detailRequest;
let detailData = null;
let opener;
const themeKey = "issue-dash-theme";
const systemTheme = matchMedia("(prefers-color-scheme: dark)");
let preferredTheme = null;

function themeStorageError(error) {
  if (!["SecurityError", "QuotaExceededError"].includes(error.name)) throw error;
  console.warn("Theme preference storage is unavailable:", error.name);
  $("theme-notice").textContent = "Your browser prevents saving theme preferences. The theme can still be changed for this page.";
  $("theme-notice").hidden = false;
}

function applyTheme() {
  if (preferredTheme) document.documentElement.dataset.theme = preferredTheme;
  else delete document.documentElement.dataset.theme;
  const dark = preferredTheme ? preferredTheme === "dark" : systemTheme.matches;
  $("theme-toggle").setAttribute("aria-pressed", String(dark));
}

try {
  const saved = localStorage.getItem(themeKey);
  if (saved === "light" || saved === "dark") preferredTheme = saved;
  else if (saved !== null) console.warn("Ignoring an unsupported saved theme preference.");
} catch (error) {
  themeStorageError(error);
}
applyTheme();
systemTheme.addEventListener("change", applyTheme);
$("theme-toggle").addEventListener("click", () => {
  preferredTheme = $("theme-toggle").getAttribute("aria-pressed") === "true" ? "light" : "dark";
  applyTheme();
  try {
    localStorage.setItem(themeKey, preferredTheme);
    $("theme-notice").hidden = true;
  } catch (error) {
    themeStorageError(error);
  }
});

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}

function label(value) {
  return value.replaceAll("-", " ").replace(/^\w/, (c) => c.toUpperCase());
}

function date(value) {
  return value ? new Date(value).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "Not recorded";
}

function badge(value) {
  return element("span", label(value), `badge ${value}`);
}

function resourceText(resource) {
  return `${resource.kind} / ${resource.namespace ? `${resource.namespace}/` : ""}${resource.name}`;
}

function filters() {
  return {
    search: $("search").value, phase: $("phase").value, severity: $("severity").value,
    family: $("family").value, namespace: $("workload-namespace").value,
    sort: $("sort").value, direction: $("direction").value,
  };
}

async function getJSON(path, signal) {
  const response = await fetch(path, {
    signal: AbortSignal.any([signal, AbortSignal.timeout(12000)]),
    cache: "no-store", credentials: "same-origin",
  });
  if (!response.headers.get("content-type")?.includes("application/json")) {
    throw new Error("The dashboard did not return JSON. Check the service connection.");
  }
  const body = await response.json();
  if (!response.ok) {
    const error = new Error(body.error?.message || `Request failed (${response.status}).`);
    error.status = response.status;
    throw error;
  }
  return body;
}

function errorText(error) {
  if (error.name === "TimeoutError") return "The request timed out. Check the service connection.";
  if (error instanceof TypeError) return "Cannot reach the dashboard. Check the port-forward or service connection.";
  return error.message || "The request failed.";
}

function updateNamespaces() {
  const select = $("workload-namespace");
  const current = select.value;
  const values = [...new Set(snapshot.items.flatMap(namespaces))].sort();
  if (current && !values.includes(current)) values.push(current);
  select.replaceChildren(new Option("All namespaces", ""));
  for (const value of values) {
    select.add(new Option(value === clusterScope ? "Cluster-scoped resources" : value, value));
  }
  select.value = current;
}

function render() {
  if (!snapshot) return;
  const focusedIssue = document.activeElement?.dataset.issue;
  const current = filters();
  const visible = selectIssues(snapshot.items, current);
  const result = paginate(visible, page);
  page = result.page;
  $("results").textContent = `${visible.length} matching / ${snapshot.items.length} stored issues${stale ? " (stale)" : ""}`;
  $("page-info").textContent = `Page ${page} of ${result.pages} · 50 per page`;
  $("previous").disabled = page === 1;
  $("next").disabled = page === result.pages;
  const counts = severityCounts(snapshot.items);
  for (const severity of severities) {
    $(`count-${severity}`).textContent = counts[severity];
    document.querySelector(`[data-severity="${severity}"]`).setAttribute("aria-pressed",
      String(current.severity === severity && current.phase === "open"));
  }
  $("counts-stale").hidden = !stale;
  const rows = result.items.map((issue) => {
    const row = element("tr");
    const severity = element("td");
    severity.append(badge(issue.severity));
    const content = element("td");
    const link = element("button", issue.title, "issue-link");
    link.type = "button";
    link.dataset.issue = issue.name;
    link.setAttribute("aria-haspopup", "dialog");
    row.addEventListener("click", () => openDetail(issue, link));
    content.append(link, element("span", resourceText(issue.anchor), "resource"),
      element("span", `${label(issue.family)} · ${issue.observer}`, "issue-meta"));
    const phase = element("td");
    phase.append(badge(issue.lifecycle.phase));
    const updated = element("td", date(issue.lifecycle.updated_at), "timestamp");
    row.append(severity, content, phase, element("td", label(issue.impact)), updated);
    return row;
  });
  $("issue-rows").replaceChildren(...rows);
  if (focusedIssue) {
    const replacement = [...document.querySelectorAll("[data-issue]")].find((node) => node.dataset.issue === focusedIssue);
    (replacement || $("refresh")).focus();
  }
  $("empty").hidden = visible.length > 0;
  $("empty-title").textContent = snapshot.items.length ? "No matching issues" : "No stored issues";
  $("empty-message").textContent = snapshot.items.length
    ? "Try another search or reset the filters."
    : "There are no ClusterIssues in this namespace. This does not establish cluster health.";
}

async function refresh() {
  if (refreshing) return;
  clearTimeout(timer);
  refreshing = true;
  $("refresh").disabled = true;
  $("refresh").textContent = "Refreshing...";
  listRequest = new AbortController();
  try {
    const next = validateSnapshot(await getJSON("/api/issues", listRequest.signal));
    snapshot = next;
    stale = false;
    $("namespace").textContent = snapshot.namespace;
    $("freshness").textContent = `Last success: ${date(snapshot.fetched_at)}`;
    $("error").hidden = true;
    updateNamespaces();
  } catch (error) {
    if (listRequest.signal.aborted) return;
    stale = true;
    $("error").textContent = `${snapshot ? "Showing stale data. " : "Issues are unavailable. "}${errorText(error)} Retrying automatically; you can also refresh manually.`;
    $("error").hidden = false;
    $("freshness").textContent = snapshot ? `Stale · last success: ${date(snapshot.fetched_at)}` : "No successful refresh";
    if (!snapshot) {
      $("namespace").textContent = "Unavailable";
      $("results").textContent = "Issues unavailable";
    }
  } finally {
    refreshing = false;
    $("refresh").disabled = false;
    $("refresh").textContent = "Refresh now";
    render();
    if (selected) loadDetail(selected);
    if (!document.hidden) timer = setTimeout(refresh, 15000);
  }
}

function section(title, text) {
  const block = element("section", undefined, "detail-section");
  block.append(element("h3", title), element("p", text, "prose"));
  return block;
}

function listSection(title, values, ordered = false) {
  const block = element("section", undefined, "detail-section");
  block.append(element("h3", title));
  if (!values.length) {
    block.append(element("p", "None recorded", "muted"));
  } else {
    const list = element(ordered ? "ol" : "ul");
    for (const value of values) list.append(element("li", value));
    block.append(list);
  }
  return block;
}

function renderDetail(data) {
  const issue = data.item;
  const expanded = $("detail-content").querySelector("details")?.open || false;
  $("detail-title").textContent = issue.title;
  $("detail-freshness").textContent = `Last success: ${date(data.fetched_at)}`;
  const tags = element("div", undefined, "detail-tags");
  tags.append(badge(issue.severity), badge(issue.lifecycle.phase), element("span", `${label(issue.impact)} impact`, "muted"));
  const facts = element("dl", undefined, "facts");
  const entries = [
    ["Issue", issue.name], ["Problem namespace", issue.anchor.namespace || "Cluster-scoped"], ["Observer", issue.observer],
    ["Problem family", label(issue.family)], ["Anchor", resourceText(issue.anchor)],
    ["Raised", date(issue.lifecycle.raised_at)], ["Updated", date(issue.lifecycle.updated_at)],
    ["Resolved", date(issue.lifecycle.resolved_at)], ["Revision", issue.lifecycle.revision],
    ["Recurrences", issue.lifecycle.recurrences],
  ];
  for (const [key, value] of entries) facts.append(element("dt", key), element("dd", String(value)));
  const provenance = element("details", undefined, "provenance");
  provenance.open = expanded;
  provenance.append(element("summary", "Evidence references and model provenance"),
    listSection("Evidence IDs (references only)", issue.analysis.evidence_ids),
    section("Model", issue.analysis.model), section("Prompt version", issue.analysis.prompt_version),
    section("Analysis ID", issue.analysis.analysis_id),
    element("p", "Only the latest stored conclusion is available, not raw evidence or revision history.", "muted"));
  $("detail-content").replaceChildren(tags, section("Diagnosis", issue.analysis.diagnosis),
    section("Explanation", issue.analysis.explanation),
    listSection("Recommendations", issue.analysis.recommendations, true),
    section("Uncertainty", issue.analysis.uncertainty),
    listSection("Affected resources", issue.resources.map(resourceText)),
    facts, provenance);
}

async function loadDetail(name) {
  detailRequest?.abort();
  const request = new AbortController();
  detailRequest = request;
  try {
    const data = validateDetail(await getJSON(`/api/issues/${encodeURIComponent(name)}`, request.signal));
    if (request.signal.aborted || selected !== name) return;
    detailData = data;
    $("detail-error").hidden = true;
    renderDetail(data);
  } catch (error) {
    if (request.signal.aborted || selected !== name) return;
    const missing = error.status === 404;
    if (missing) {
      detailData = null;
      $("detail-content").replaceChildren();
    }
    $("detail-error").textContent = `${detailData ? "Showing stale details. " : ""}${errorText(error)}`;
    $("detail-error").hidden = false;
    $("detail-freshness").textContent = detailData
      ? `Stale · last success: ${date(detailData.fetched_at)}`
      : missing ? "Issue unavailable" : "No successful detail refresh";
  }
}

function openDetail(issue, trigger) {
  opener = trigger;
  selected = issue.name;
  detailData = null;
  $("detail-title").textContent = issue.title;
  $("detail-freshness").textContent = "Loading latest details...";
  $("detail-error").hidden = true;
  $("detail-content").replaceChildren();
  $("detail").showModal();
  loadDetail(issue.name);
}

$("close-detail").addEventListener("click", () => $("detail").close());
$("detail").addEventListener("click", (event) => {
  const dialog = event.currentTarget;
  if (event.target !== dialog) return;
  const bounds = dialog.getBoundingClientRect();
  if (event.clientX < bounds.left || event.clientX >= bounds.right ||
      event.clientY < bounds.top || event.clientY >= bounds.bottom) {
    dialog.close();
  }
});
$("detail").addEventListener("close", () => {
  const name = selected;
  selected = null;
  detailData = null;
  detailRequest?.abort();
  const replacement = [...document.querySelectorAll("[data-issue]")].find((node) => node.dataset.issue === name);
  (opener?.isConnected ? opener : replacement || $("refresh")).focus();
});
$("filters").addEventListener("submit", (event) => event.preventDefault());
for (const id of ["search", "phase", "severity", "family", "workload-namespace"]) {
  $(id).addEventListener(id === "search" ? "input" : "change", () => { page = 1; render(); });
}
$("sort").addEventListener("change", () => {
  $("direction").value = $("sort").value === "title" ? "asc" : "desc";
  page = 1;
  render();
});
$("direction").addEventListener("change", () => { page = 1; render(); });
$("reset-filters").addEventListener("click", () => { $("filters").reset(); page = 1; render(); });
for (const card of document.querySelectorAll("[data-severity]")) {
  card.addEventListener("click", () => {
    $("severity").value = $("severity").value === card.dataset.severity ? "" : card.dataset.severity;
    $("phase").value = "open";
    page = 1;
    render();
  });
}
$("previous").addEventListener("click", () => { page--; render(); });
$("next").addEventListener("click", () => { page++; render(); });
$("refresh").addEventListener("click", refresh);
document.addEventListener("visibilitychange", () => {
  clearTimeout(timer);
  if (!document.hidden) refresh();
});
window.addEventListener("pagehide", () => {
  clearTimeout(timer);
  listRequest?.abort();
  detailRequest?.abort();
});
refresh();
