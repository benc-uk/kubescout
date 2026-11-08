import { severities, clusterScope, namespaces, selectIncidents, paginate, severityCounts, validateSnapshot, validateDetail } from "./model.js";

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
let selectedDescription = null;
let opener;
let triageRequest;
const detailTabs = ["overview", "resources", "record"];
const themeKey = "kubescout-dashboard-theme";
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
  return `${resource.kind}${resource.api_group ? ` (${resource.api_group})` : ""} / ${resource.namespace ? `${resource.namespace}/` : ""}${resource.name}`;
}

function copyable(value, description, displayValue = value) {
  const group = element("span", undefined, "copyable");
  const text = element("code", displayValue, "copy-value");
  const button = element("button", undefined, "copy-button");
  button.type = "button";
  button.title = `Copy ${description}`;
  button.setAttribute("aria-label", `Copy ${description}`);
  const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  icon.setAttribute("viewBox", "0 0 24 24");
  icon.setAttribute("width", "16");
  icon.setAttribute("height", "16");
  icon.setAttribute("aria-hidden", "true");
  icon.setAttribute("focusable", "false");
  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", "M9 9h11v11H9zM5 15H4V4h11v1");
  path.setAttribute("fill", "none");
  path.setAttribute("stroke", "currentColor");
  path.setAttribute("stroke-width", "1.8");
  path.setAttribute("stroke-linejoin", "round");
  icon.append(path);
  button.append(icon);
  const feedback = element("span", undefined, "copy-feedback");
  feedback.setAttribute("role", "status");
  feedback.hidden = true;
  let feedbackTimer;
  const report = (message, temporary = false) => {
    clearTimeout(feedbackTimer);
    feedback.textContent = message;
    feedback.hidden = false;
    if (temporary) feedbackTimer = setTimeout(() => { feedback.hidden = true; }, 2000);
  };
  const selectManually = (message) => {
    text.textContent = value;
    const selection = document.getSelection();
    if (!selection) {
      report(`${message} Select the value and copy it manually.`);
      return;
    }
    const range = document.createRange();
    range.selectNodeContents(text);
    selection.removeAllRanges();
    selection.addRange(range);
    report(`${message} Text selected; press Ctrl+C or Cmd+C.`);
  };
  button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      if (!navigator.clipboard?.writeText) {
        selectManually("Clipboard unavailable.");
        return;
      }
      await navigator.clipboard.writeText(value);
      report("Copied", true);
    } catch (error) {
      if (!["NotAllowedError", "SecurityError"].includes(error.name)) throw error;
      selectManually("Browser blocked copying.");
    } finally {
      button.disabled = false;
    }
  });
  group.append(text, button, feedback);
  return group;
}

function filters() {
  return {
    search: $("search").value, state: $("state").value, severity: $("severity").value,
    family: $("family").value, namespace: $("workload-namespace").value,
    sort: $("sort").value, direction: $("direction").value,
  };
}

function setDirection(direction) {
  const button = $("direction");
  const ascending = direction === "asc";
  const orders = {
    severity: ["Lowest severity first", "Highest severity first"],
    updated: ["Oldest update first", "Newest update first"],
    raised: ["Oldest incident first", "Newest incident first"],
    title: ["Title A to Z", "Title Z to A"],
  }[$("sort").value];
  button.value = direction;
  const description = `${orders[ascending ? 0 : 1]}. Switch to ${orders[ascending ? 1 : 0].toLowerCase()}.`;
  button.setAttribute("aria-label", description);
  button.title = description;
}

async function getJSON(path, signal, options = {}) {
  const response = await fetch(path, {
    ...options,
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
  const values = [...new Set(snapshot.incidents.flatMap(namespaces))].sort();
  if (current && !values.includes(current)) values.push(current);
  select.replaceChildren(new Option("All namespaces", ""));
  for (const value of values) {
    select.add(new Option(value === clusterScope ? "Cluster-scoped resources" : value, value));
  }
  select.value = current;
}

function render() {
  if (!snapshot) return;
  const focusedIncident = document.activeElement?.dataset.incident;
  const current = filters();
  const visible = selectIncidents(snapshot.incidents, current);
  const result = paginate(visible, page);
  page = result.page;
  $("results").textContent = `${visible.length} matching / ${snapshot.incidents.length} stored incidents${stale ? " (stale)" : ""}`;
  $("page-info").textContent = `Page ${page} of ${result.pages} · 50 per page`;
  $("previous").disabled = page === 1;
  $("next").disabled = page === result.pages;
  const counts = severityCounts(snapshot.incidents);
  for (const severity of severities) {
    $(`count-${severity}`).textContent = counts[severity];
    document.querySelector(`[data-severity="${severity}"]`).setAttribute("aria-pressed",
      String(current.severity === severity && current.state === "open"));
  }
  $("counts-stale").hidden = !stale;
  const rows = result.items.map((incident) => {
    const row = element("tr");
    const severity = element("td");
    severity.append(badge(incident.severity));
    const content = element("td");
    const link = element("button", incident.title, "incident-link");
    link.type = "button";
    link.dataset.incident = incident.name;
    link.setAttribute("aria-haspopup", "dialog");
    row.addEventListener("click", () => openDetail(incident, link));
    content.append(link, element("span", resourceText(incident.anchor), "resource"),
      element("span", `${label(incident.family)} · ${incident.observer}`, "incident-meta"));
    const state = element("td");
    state.append(badge(incident.dismissed ? "dismissed" : incident.lifecycle.state));
    const updated = element("td", date(incident.lifecycle.updated_at), "timestamp");
    row.append(severity, content, state, element("td", label(incident.impact)), updated);
    return row;
  });
  $("incident-rows").replaceChildren(...rows);
  if (focusedIncident) {
    const replacement = [...document.querySelectorAll("[data-incident]")].find((node) => node.dataset.incident === focusedIncident);
    (replacement || $("refresh")).focus();
  }
  $("empty").hidden = visible.length > 0;
  $("empty-title").textContent = snapshot.incidents.length ? "No matching incidents" : "No stored incidents";
  $("empty-message").textContent = snapshot.incidents.length
    ? "Try another search or reset the filters."
    : "There are no stored Incidents in this cluster. This does not establish cluster health.";
}

async function refresh() {
  if (refreshing || triageRequest) return;
  clearTimeout(timer);
  refreshing = true;
  updateTriageAction();
  $("refresh").disabled = true;
  $("refresh").textContent = "Refreshing...";
  listRequest = new AbortController();
  try {
    const next = validateSnapshot(await getJSON("/api/incidents", listRequest.signal));
    snapshot = next;
    stale = false;
    $("freshness").textContent = `Last success: ${date(snapshot.fetched_at)}`;
    $("error").hidden = true;
    updateNamespaces();
  } catch (error) {
    if (listRequest.signal.aborted) return;
    stale = true;
    $("error").textContent = `${snapshot ? "Showing stale data. " : "Incidents are unavailable. "}${errorText(error)} Retrying automatically; you can also refresh manually.`;
    $("error").hidden = false;
    $("freshness").textContent = snapshot ? `Stale · last success: ${date(snapshot.fetched_at)}` : "No successful refresh";
    if (!snapshot) {
      $("results").textContent = "Incidents unavailable";
    }
  } finally {
    refreshing = false;
    $("refresh").disabled = false;
    $("refresh").textContent = "Refresh now";
    render();
    if (selected && !triageRequest) loadDetail(selected);
    updateTriageAction();
    if (!document.hidden) timer = setTimeout(refresh, 15000);
  }
}

function section(title, text, className = "") {
  const block = element("section", undefined, `detail-section detail-card ${className}`.trim());
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
    for (const value of values) {
      const item = element("li");
      item.append(element("span", value));
      list.append(item);
    }
    block.append(list);
  }
  return block;
}

function factList(entries) {
  const facts = element("dl", undefined, "facts");
  for (const [key, value] of entries) {
    const pair = element("div");
    const entry = element("dd");
    entry.append(value instanceof Node ? value : String(value));
    pair.append(element("dt", key), entry);
    facts.append(pair);
  }
  return facts;
}

function descriptionStatus(title, section) {
  const status = element("span", `${title}: ${label(section.status)}`, "description-status");
  status.dataset.status = section.status;
  return status;
}

function descriptionPanel(title, section, evidenceID, resourceRef) {
  const panel = element("section", undefined, "description-panel");
  const heading = element("div", undefined, "description-panel-heading");
  const content = copyable(section.text, `${evidenceID} ${title.toLowerCase()}`);
  const button = content.querySelector(".copy-button");
  button.append(element("span", "Copy"));
  heading.append(element("h5", title), descriptionStatus("Coverage", section), button);
  const text = content.querySelector(".copy-value");
  text.tabIndex = 0;
  text.setAttribute("role", "region");
  text.setAttribute("aria-label", `${title} description text`);
  text.classList.add("native-describe");
  text.dataset.descriptionFocus = `output:${resourceRef}`;
  panel.append(heading, content);
  return panel;
}

function updateDescriptionStatus() {
  $("description-view-freshness").textContent = $("detail-freshness").textContent;
  $("description-view-error").textContent = $("detail-error").textContent;
  $("description-view-error").hidden = $("detail-error").hidden;
}

function renderDescriptionView(data) {
  if (!selectedDescription || !$("description-view").open) return;
  const incident = data.incident;
  const description = incident.evidence?.descriptions.find((entry) => entry.resource.ref === selectedDescription.resourceRef);
  if (selectedDescription.revisionKey !== `${incident.name}:${incident.lifecycle.revision}` || !description) {
    $("description-view").close();
    return;
  }
  const previous = $("description-view-content").querySelector(".native-describe");
  const scrollTop = previous?.scrollTop || 0;
  const scrollLeft = previous?.scrollLeft || 0;
  const focusedOutput = document.activeElement === previous;
  const focusedCopy = $("description-view-content").contains(document.activeElement)
    && !!document.activeElement.closest(".copy-button");
  const panel = descriptionPanel("kubectl describe", description.output, description.evidence_id, description.resource.ref);
  $("description-view-content").replaceChildren(panel);
  $("description-view-resource").textContent = resourceText(description.resource);
  $("description-view-capture").textContent = `Captured during investigation, not live status · Revision: ${incident.lifecycle.revision} · Observed: ${date(description.observed_at)} · Evidence: ${description.evidence_id}`;
  updateDescriptionStatus();
  const output = panel.querySelector(".native-describe");
  if (focusedOutput) output.focus({ preventScroll: true });
  if (focusedCopy) panel.querySelector(".copy-button").focus({ preventScroll: true });
  output.scrollTop = scrollTop;
  output.scrollLeft = scrollLeft;
}

function openDescriptionView(description, revisionKey) {
  selectedDescription = { resourceRef: description.resource.ref, revisionKey };
  $("description-view-content").replaceChildren();
  $("description-view").showModal();
  renderDescriptionView(detailData);
}

function resourceDescriptions(evidence, revisionKey) {
  const previous = $("detail-resources").querySelector(".resource-descriptions");
  const expanded = new Set(previous?.dataset.revisionKey === revisionKey
    ? [...previous.querySelectorAll(".description-entry[open]")].map((entry) => entry.dataset.resourceRef) : []);
  const block = element("section", undefined, "detail-section resource-descriptions");
  block.dataset.revisionKey = revisionKey;
  const header = element("div", undefined, "description-toolbar");
  header.append(element("h3", "Resource descriptions"));
  block.append(header);
  if (!evidence) {
    block.append(element("p", "No descriptions captured for this incident revision.", "muted"));
    return block;
  }
  const actions = element("div", undefined, "description-actions");
  for (const [text, open] of [["Expand all", true], ["Collapse all", false]]) {
    const button = element("button", text, "secondary");
    button.type = "button";
    button.setAttribute("aria-label", `${text} resource descriptions`);
    button.dataset.descriptionFocus = open ? "expand" : "collapse";
    button.addEventListener("click", () => {
      for (const entry of block.querySelectorAll(".description-entry")) entry.open = open;
    });
    actions.append(button);
  }
  header.append(actions);
  block.append(element("p", `${evidence.descriptions.length} resources captured; ${evidence.omitted_resources} additional context resources omitted.`, "description-coverage"),
    element("p", "Captured during investigation, not live status. Expand a resource for its kubectl describe output.", "muted description-intro"));
  for (const description of evidence.descriptions) {
    const entry = element("details", undefined, "description-entry");
    entry.dataset.resourceRef = description.resource.ref;
    entry.open = expanded.has(description.resource.ref);
    const summary = element("summary", undefined, "description-summary");
    summary.dataset.descriptionFocus = `resource:${description.resource.ref}`;
    const identity = element("span", undefined, "description-identity");
    identity.append(element("span", `${description.resource.kind}${description.resource.api_group ? ` (${description.resource.api_group})` : ""}`, "description-kind"),
      element("strong", description.resource.name, "description-name"),
      element("span", description.resource.namespace ? `Namespace: ${description.resource.namespace}` : "Cluster-scoped", "description-namespace"));
    const statuses = element("span", undefined, "description-statuses");
    statuses.append(descriptionStatus("Describe", description.output));
    summary.append(identity, statuses);
    const body = element("div", undefined, "description-body");
    const panel = descriptionPanel("kubectl describe", description.output, description.evidence_id, description.resource.ref);
    const expand = element("button", undefined, "secondary description-expand");
    expand.type = "button";
    expand.setAttribute("aria-label", `View ${resourceText(description.resource)} kubectl describe full screen`);
    expand.setAttribute("aria-haspopup", "dialog");
    expand.dataset.descriptionFocus = `fullscreen:${description.resource.ref}`;
    const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    icon.setAttribute("viewBox", "0 0 24 24");
    icon.setAttribute("width", "16");
    icon.setAttribute("height", "16");
    icon.setAttribute("aria-hidden", "true");
    icon.setAttribute("focusable", "false");
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", "M8 3H3v5m13-5h5v5M3 16v5h5m13-5v5h-5M3 3l6 6m12-6-6 6M3 21l6-6m12 6-6-6");
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", "currentColor");
    path.setAttribute("stroke-width", "1.8");
    path.setAttribute("stroke-linecap", "round");
    path.setAttribute("stroke-linejoin", "round");
    icon.append(path);
    expand.append(icon, element("span", "Full screen"));
    expand.addEventListener("click", () => openDescriptionView(description, revisionKey));
    panel.querySelector(".description-panel-heading").append(expand);
    body.append(element("p", `${description.source} · Observed: ${date(description.observed_at)} · Evidence: ${description.evidence_id}`, "muted description-metadata"),
      panel);
    entry.append(summary, body);
    block.append(entry);
  }
  block.append(element("p", "Native kubectl describe retains operational details with credential values redacted. Annotations, environment values, UIDs, Pod logs and Normal events are excluded. Reads and text are scoped and size-limited. Copy preserves the original stored text.", "muted description-exclusions"));
  return block;
}

function selectDetailTab(name) {
  for (const tab of detailTabs) {
    const active = name === tab;
    $(`detail-tab-${tab}`).setAttribute("aria-selected", String(active));
    $(`detail-tab-${tab}`).tabIndex = active ? 0 : -1;
    $(`detail-${tab}`).hidden = !active;
  }
  $("detail-body").scrollTop = 0;
}

function clearDetail() {
  $("description-view").close();
  if ($("detail-content").contains(document.activeElement) || $("detail-tabs").contains(document.activeElement)) {
    $("close-detail").focus();
  }
  $("detail-content").hidden = true;
  $("detail-tabs").hidden = true;
  $("detail-tags").hidden = true;
  $("detail-tags").replaceChildren();
  $("detail-resource").hidden = true;
  $("detail-resource").textContent = "";
  $("detail-resource-count").textContent = "";
  for (const tab of detailTabs) $(`detail-${tab}`).replaceChildren();
  selectDetailTab("overview");
}

function renderDetail(data) {
  const incident = data.incident;
  const revisionKey = `${incident.name}:${incident.lifecycle.revision}`;
  const previousDescriptions = $("detail-resources").querySelector(".resource-descriptions");
  const outputScroll = new Map(previousDescriptions?.dataset.revisionKey === revisionKey
    ? [...previousDescriptions.querySelectorAll(".native-describe")].map((node) =>
      [node.dataset.descriptionFocus, { top: node.scrollTop, left: node.scrollLeft }]) : []);
  const scrollTop = $("detail-body").scrollTop;
  const focusedCopy = $("detail-content").contains(document.activeElement)
    ? document.activeElement.closest(".copy-button")?.getAttribute("aria-label") : null;
  const focusedDescription = $("detail-resources").contains(document.activeElement)
    ? document.activeElement.dataset.descriptionFocus : null;
  $("detail-title").textContent = incident.title;
  $("detail-freshness").textContent = `Last success: ${date(data.fetched_at)}`;
  $("detail-tags").replaceChildren(badge(incident.severity), badge(incident.dismissed ? "dismissed" : incident.lifecycle.state),
    element("span", `${label(incident.impact)} impact`, "muted"));
  $("detail-tags").hidden = false;
  $("detail-resource").textContent = resourceText(incident.anchor);
  $("detail-resource").hidden = false;
  $("detail-resource-count").textContent = incident.resources.length;
  const overview = element("div", undefined, "detail-overview-grid");
  const recommendations = listSection("Recommendations", incident.analysis.recommendations, true);
  recommendations.classList.add("detail-card");
  overview.append(section("Explanation", incident.analysis.explanation), recommendations);
  $("detail-overview").replaceChildren(section("AI diagnosis", incident.analysis.diagnosis, "detail-diagnosis"),
    overview, section("What is uncertain", incident.analysis.uncertainty, "detail-uncertainty"));
  const resources = listSection("Affected resources", incident.resources.map(resourceText));
  resources.classList.add("detail-card", "detail-resources");
  const descriptions = resourceDescriptions(incident.evidence, revisionKey);
  $("detail-resources").replaceChildren(resources, descriptions);
  const facts = factList([
    ["Incident", copyable(incident.name, "incident name")],
    ["Problem namespace", incident.anchor.namespace ? copyable(incident.anchor.namespace, "namespace") : "Cluster-scoped"],
    ["Resource kind", incident.anchor.kind], ["Resource name", copyable(incident.anchor.name, "resource name")],
    ["Observer", copyable(incident.observer, "observer ID")], ["Problem family", label(incident.family)],
    ["Raised", date(incident.lifecycle.raised_at)], ["Updated", date(incident.lifecycle.updated_at)],
    ["Resolved", date(incident.lifecycle.resolved_at)], ["Revision", incident.lifecycle.revision],
    ["Recurrences", incident.lifecycle.recurrences],
  ]);
  const provenance = element("section", undefined, "provenance");
  provenance.append(element("h3", "Evidence references and model provenance"),
    listSection("Evidence IDs", incident.analysis.evidence_ids),
    factList([["Model", incident.analysis.model], ["Prompt version", incident.analysis.prompt_version],
      ["Analysis ID", incident.analysis.analysis_id]]),
    element("p", "Resource descriptions, when captured, are on the Resources tab. Other evidence IDs remain references only. No raw objects or revision history are available.", "muted"));
  const record = element("section", undefined, "detail-card");
  record.append(element("h3", "Incident record"), facts, provenance);
  $("detail-record").replaceChildren(record);
  $("detail-content").hidden = false;
  $("detail-tabs").hidden = false;
  $("detail-loading").hidden = true;
  $("detail-body").scrollTop = scrollTop;
  if (focusedCopy) {
    [...$("detail-content").querySelectorAll(".copy-button")].find((button) =>
      button.getAttribute("aria-label") === focusedCopy)?.focus({ preventScroll: true });
  }
  if (focusedDescription) {
    [...descriptions.querySelectorAll("[data-description-focus]")].find((node) =>
      node.dataset.descriptionFocus === focusedDescription)?.focus({ preventScroll: true });
  }
  for (const node of descriptions.querySelectorAll(".native-describe")) {
    const position = outputScroll.get(node.dataset.descriptionFocus);
    if (position) {
      node.scrollTop = position.top;
      node.scrollLeft = position.left;
    }
  }
  renderDescriptionView(data);
  updateTriageAction();
}

for (const [index, name] of detailTabs.entries()) {
  const button = $(`detail-tab-${name}`);
  button.addEventListener("click", () => selectDetailTab(name));
  button.addEventListener("keydown", (event) => {
    let next;
    if (event.key === "ArrowRight") next = (index + 1) % detailTabs.length;
    else if (event.key === "ArrowLeft") next = (index + detailTabs.length - 1) % detailTabs.length;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = detailTabs.length - 1;
    else return;
    event.preventDefault();
    selectDetailTab(detailTabs[next]);
    $(`detail-tab-${detailTabs[next]}`).focus();
  });
}

function updateTriageAction() {
  $("refresh").disabled = !!triageRequest || refreshing;
  const action = $("triage-action");
  action.hidden = !detailData;
  action.disabled = !detailData || !!triageRequest || refreshing || stale || !$("detail-error").hidden;
  action.textContent = triageRequest ? "Saving..." : detailData?.incident.dismissed ? "Restore incident" : "Dismiss incident";
}

$("triage-action").addEventListener("click", async () => {
  if (!detailData || $("triage-action").disabled) return;
  const incident = detailData.incident;
  const request = new AbortController();
  triageRequest = request;
  detailRequest?.abort();
  clearTimeout(timer);
  $("triage-error").hidden = true;
  updateTriageAction();
  try {
    const data = validateDetail(await getJSON(`/api/incidents/${encodeURIComponent(incident.name)}/triage`, request.signal, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-KubeScout-Action": "triage" },
      body: JSON.stringify({ version: incident.version, dismissed: !incident.dismissed }),
    }));
    if (selected === incident.name) {
      detailData = data;
      renderDetail(data);
    }
  } catch (error) {
    if (selected === incident.name) {
      $("triage-error").textContent = `${errorText(error)} Refresh and review the current status before retrying.`;
      $("triage-error").hidden = false;
    }
  } finally {
    triageRequest = null;
    await refresh();
    updateTriageAction();
  }
});

async function loadDetail(name) {
  detailRequest?.abort();
  const request = new AbortController();
  detailRequest = request;
  try {
    const data = validateDetail(await getJSON(`/api/incidents/${encodeURIComponent(name)}`, request.signal));
    if (request.signal.aborted || selected !== name) return;
    detailData = data;
    $("detail-error").hidden = true;
    renderDetail(data);
  } catch (error) {
    if (request.signal.aborted || selected !== name) return;
    const missing = error.status === 404;
    if (missing) {
      detailData = null;
      clearDetail();
    }
    $("detail-loading").hidden = true;
    $("detail-error").textContent = `${detailData ? "Showing stale details. " : ""}${errorText(error)}`;
    $("detail-error").hidden = false;
    $("detail-freshness").textContent = detailData
      ? `Stale · last success: ${date(detailData.fetched_at)}`
      : missing ? "Incident unavailable" : "No successful detail refresh";
    updateDescriptionStatus();
    updateTriageAction();
  }
}

function openDetail(incident, trigger) {
  opener = trigger;
  selected = incident.name;
  detailData = null;
  $("detail-title").textContent = incident.title;
  $("detail-freshness").textContent = "Loading latest details...";
  $("detail-error").hidden = true;
  $("triage-error").hidden = true;
  clearDetail();
  $("detail-loading").hidden = false;
  updateTriageAction();
  $("detail").showModal();
  loadDetail(incident.name);
}

$("close-detail").addEventListener("click", () => $("detail").close());
$("close-detail-footer").addEventListener("click", () => $("detail").close());
$("close-description-view").addEventListener("click", () => $("description-view").close());
$("description-view").addEventListener("close", () => {
  if ($("description-view").open) return;
  const resourceRef = selectedDescription?.resourceRef;
  selectedDescription = null;
  $("description-view-content").replaceChildren();
  if (!$("detail").open) return;
  const entry = [...$("detail-resources").querySelectorAll(".description-entry")].find((node) =>
    node.dataset.resourceRef === resourceRef);
  (entry?.open ? entry.querySelector(".description-expand") : entry?.querySelector("summary") || $("close-detail")).focus({ preventScroll: true });
});
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
  if ($("detail").open) return;
  $("description-view").close();
  const name = selected;
  selected = null;
  detailData = null;
  updateTriageAction();
  detailRequest?.abort();
  const replacement = [...document.querySelectorAll("[data-incident]")].find((node) => node.dataset.incident === name);
  (opener?.isConnected ? opener : replacement || $("refresh")).focus();
});
$("filters").addEventListener("submit", (event) => event.preventDefault());
for (const id of ["search", "state", "severity", "family", "workload-namespace"]) {
  $(id).addEventListener(id === "search" ? "input" : "change", () => { page = 1; render(); });
}
$("sort").addEventListener("change", () => {
  setDirection($("sort").value === "title" ? "asc" : "desc");
  page = 1;
  render();
});
$("direction").addEventListener("click", () => {
  setDirection($("direction").value === "desc" ? "asc" : "desc");
  page = 1;
  render();
});
$("reset-filters").addEventListener("click", () => { $("filters").reset(); page = 1; render(); });
for (const card of document.querySelectorAll("[data-severity]")) {
  card.addEventListener("click", () => {
    $("severity").value = $("severity").value === card.dataset.severity ? "" : card.dataset.severity;
    $("state").value = "open";
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
  triageRequest?.abort();
});
refresh();
