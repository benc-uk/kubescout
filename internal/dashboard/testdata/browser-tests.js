import { selectIssues, paginate, severityCounts, namespaces, clusterScope, validateSnapshot, validateDetail } from "/model.js";

const $ = (id) => document.getElementById(id);
const results = [];
const scriptErrors = [];
window.addEventListener("error", (event) => scriptErrors.push(event.message));
window.addEventListener("unhandledrejection", (event) => scriptErrors.push(String(event.reason)));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
function assert(value, message) {
  if (!value) throw new Error(message);
  results.push(`PASS ${message}`);
  $("test-results").textContent = results.join("\n");
}
async function until(predicate, message, timeout = 4000) {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeout) throw new Error(`Timed out: ${message}`);
    await sleep(20);
  }
}
async function state(mode) {
  const response = await fetch("/__test/state", { method: "POST", body: JSON.stringify({ mode }) });
  if (!response.ok) throw new Error("Cannot set fixture state");
}
function set(id, value) {
  $(id).value = value;
  $(id).dispatchEvent(new Event(id === "search" ? "input" : "change", { bubbles: true }));
}
function titles() {
  return [...document.querySelectorAll(".issue-link")].map((node) => node.textContent);
}
async function refresh() {
  await until(() => !$("refresh").disabled, "refresh available");
  $("refresh").click();
  await until(() => !$("refresh").disabled, "refresh finished");
}

function contrast(node) {
  const rgb = (color) => color.match(/[\d.]+/g).slice(0, 3).map(Number);
  const luminance = (color) => rgb(color).map((value) => {
    const channel = value / 255;
    return channel <= .04045 ? channel / 12.92 : ((channel + .055) / 1.055) ** 2.4;
  }).reduce((sum, value, index) => sum + value * [.2126, .7152, .0722][index], 0);
  let background = node;
  while (getComputedStyle(background).backgroundColor === "rgba(0, 0, 0, 0)") background = background.parentElement;
  const values = [luminance(getComputedStyle(node).color), luminance(getComputedStyle(background).backgroundColor)];
  return (Math.max(...values) + .05) / (Math.min(...values) + .05);
}

try {
  await state("normal");
  await until(() => $("results").textContent.includes("stored issues"), "initial render");
  assert($("page-title").textContent === "CLUSTER ISSUES", "uppercase page title");
  assert(!document.querySelector(".topbar, .subtitle, .footnote, .row-arrow, .page-heading .eyebrow"),
    "marked header, subtitle, footer and decorative chevrons removed");
  assert(document.querySelectorAll("thead th").length === 5 && document.querySelector("#issue-rows tr").children.length === 5,
    "no unused chevron column");
  assert(parseFloat(getComputedStyle(document.querySelector(".issue-link")).fontSize) >= 17
    && parseFloat(getComputedStyle($("search")).fontSize) >= 16
    && parseFloat(getComputedStyle(document.querySelector(".issue-meta")).fontSize) >= 14,
    "larger issue, control and secondary text");
  const previousTheme = localStorage.getItem("issue-dash-theme");
  const initialDark = $("theme-toggle").getAttribute("aria-pressed") === "true";
  for (const theme of ["light", "dark"]) {
    if (($("theme-toggle").getAttribute("aria-pressed") === "true") !== (theme === "dark")) $("theme-toggle").click();
    assert(getComputedStyle(document.documentElement).colorScheme.includes(theme), `${theme} theme applies to native controls`);
    const readable = [...document.querySelectorAll(".count-card, .badge, .connection, th, .issue-link, .issue-meta, #theme-toggle, #error")]
      .every((node) => contrast(node) >= 4.5);
    assert(readable, `${theme} theme text contrast is at least 4.5:1`);
  }
  assert(localStorage.getItem("issue-dash-theme") === "dark", "explicit theme choice is saved");
  const setItem = Storage.prototype.setItem;
  try {
    Storage.prototype.setItem = () => { throw new DOMException("Fixture storage denial", "SecurityError"); };
    $("theme-toggle").click();
    assert(document.documentElement.dataset.theme === "light" && !$("theme-notice").hidden,
      "blocked storage keeps theme switching usable and reports the limitation");
  } finally {
    Storage.prototype.setItem = setItem;
  }
  $("theme-toggle").click();
  assert($("theme-notice").hidden, "successful theme persistence clears the storage warning");
  if (($("theme-toggle").getAttribute("aria-pressed") === "true") !== initialDark) $("theme-toggle").click();
  if (previousTheme === null) localStorage.removeItem("issue-dash-theme");
  else localStorage.setItem("issue-dash-theme", previousTheme);
  const data = validateSnapshot(await (await fetch("/api/issues")).json());
  const defaults = { search: "", phase: "open", severity: "", family: "", namespace: "", sort: "severity", direction: "desc" };
  const selected = selectIssues(data.items, defaults);
  assert(selected.length === 60 && selected[0].name === "issue-056", "open-only default with severity and newest-first tie-breaker");
  assert(selectIssues(data.items, { ...defaults, direction: "asc" })[0].severity === "low", "numeric ascending severity");
  assert(selectIssues(data.items, { ...defaults, sort: "title", direction: "asc" })[0].name === "issue-000", "title sorting");
  assert(selectIssues(data.items, { ...defaults, sort: "updated" })[0].name === "issue-059", "updated sorting");
  assert(selectIssues(data.items, { ...defaults, phase: "resolved" }).length === 4, "resolved filtering");
  assert(selectIssues(data.items, { ...defaults, search: "OTHER-OBSERVER", namespace: "payments", severity: "medium" }).length === 1, "combined case-insensitive search and filters");
  const multiNamespaceIssue = { ...data.items[0], anchor: { kind: "Deployment", namespace: "apps", name: "checkout" },
    resources: [{ kind: "Pod", namespace: "payments", name: "worker" }] };
  assert(selectIssues([multiNamespaceIssue], { ...defaults, namespace: "payments" }).length === 1
    && selectIssues([multiNamespaceIssue], { ...defaults, namespace: "apps" }).length === 1
    && selectIssues([multiNamespaceIssue], { ...defaults, namespace: "unrelated" }).length === 0,
    "namespace filter matches anchors and affected workloads across namespaces");
  assert(namespaces(data.items[1]).includes(clusterScope), "cluster-scoped resource filter");
  assert(paginate(selected, 1).items.length === 50 && paginate(selected, 999).page === 2, "50-row pagination and clamping");
  assert(severityCounts(data.items).critical === 15, "counts exclude resolved issues");
  let rejected = false;
  try { validateSnapshot({ items: null }); } catch { rejected = true; }
  assert(rejected, "malformed snapshot rejected");
  validateDetail(await (await fetch("/api/issues/issue-000")).json());
  assert(document.querySelectorAll(".issue-link").length === 50 && $("next").disabled === false, "table starts on first page");
  $("next").click();
  assert(document.querySelectorAll(".issue-link").length === 10 && $("page-info").textContent.includes("Page 2"), "next page shows remaining rows");
  set("severity", "critical");
  assert(titles().length === 15 && $("page-info").textContent.includes("Page 1"), "filter resets page");
  set("family", "node-health");
  assert(!$("empty").hidden && $("empty-title").textContent === "No matching issues", "combined filters and empty state");
  $("reset-filters").click();
  set("family", "service-availability");
  assert(titles().length === 1 && titles()[0].includes("03"), "service availability family filter");
  $("reset-filters").click();
  set("workload-namespace", "payments");
  assert(titles().length === selectIssues(data.items, { ...defaults, namespace: "payments" }).length
    && $("namespace").textContent === data.namespace, "namespace dropdown filters workloads without changing issue-storage namespace");
  $("reset-filters").click();
  set("workload-namespace", clusterScope);
  assert(titles().length === 1 && titles()[0].includes("01"), "workload namespace distinguishes cluster-scoped resources");
  $("reset-filters").click();
  set("phase", "resolved");
  assert(titles().length === 4 && document.querySelectorAll(".badge.resolved").length === 4, "resolved UI view");
  $("reset-filters").click();
  set("search", "onerror");
  assert(titles().length === 1 && !document.querySelector("#issue-rows img"), "untrusted title rendered as text");
  document.querySelector(".issue-link").click();
  await until(() => $("detail-content").textContent.includes("Recommendations"), "details loaded");
  const problemNamespace = [...$("detail-content").querySelectorAll("dt")].find((node) => node.textContent === "Problem namespace");
  assert(problemNamespace?.nextElementSibling?.textContent === "apps"
    && !$("detail-content").textContent.includes("Issue namespace"), "details show problem namespace instead of issue-storage namespace");
  assert(parseFloat(getComputedStyle(document.querySelector(".detail-section p")).fontSize) >= 16, "larger detail text");
  assert($("detail").open && !$("detail-content").querySelector("script") && !window.injected, "safe drill-down and untrusted recommendations");
  assert($("detail-content").textContent.includes("Evidence IDs") && $("detail-content").textContent.includes("Revision"), "evidence and lifecycle details");
  $("detail-content").querySelector("details").open = true;
  await refresh();
  await until(() => $("detail-content").querySelector("details")?.open, "detail disclosure retained");
  assert($("search").value === "onerror" && $("detail").open, "selection and filters preserved on refresh");
  await state("error");
  await refresh();
  await until(() => !$("detail-error").hidden, "detail failure");
  assert(!$("error").hidden && $("error").textContent.includes("stale") && titles().length === 1, "failed refresh keeps stale rows");
  assert($("detail-error").textContent.includes("stale") && $("detail-content").textContent.includes("Recommendations"), "failed detail refresh keeps stale content");
  await state("deleted");
  await refresh();
  await until(() => $("detail-freshness").textContent === "Issue unavailable", "deleted detail");
  assert($("detail-content").textContent === "", "deleted issue clears detail rather than showing current data");
  $("close-detail").click();
  await until(() => document.activeElement?.className === "issue-link", "focus restoration");
  assert(!$("detail").open, "detail closes and restores focus");
  await state("slow");
  $("reset-filters").click();
  document.querySelector(".issue-link").click();
  $("close-detail").click();
  await until(() => !$("detail").open, "old detail closed");
  set("search", "Workload 01");
  await state("normal");
  document.querySelector(".issue-link").click();
  await until(() => $("detail-content").textContent.includes("worker-1"), "new selection");
  const nodeNamespace = [...$("detail-content").querySelectorAll("dt")].find((node) => node.textContent === "Problem namespace");
  assert(nodeNamespace?.nextElementSibling?.textContent === "Cluster-scoped", "node problems display cluster scope rather than issue-storage namespace");
  await sleep(600);
  assert($("detail-content").textContent.includes("worker-1"), "obsolete detail response cannot replace newer selection");
  $("close-detail").click();
  $("reset-filters").click();
  await state("empty");
  await refresh();
  assert(!$("empty").hidden && $("empty-title").textContent === "No stored issues", "empty snapshot differs from failure");
  await state("normal");
  const before = (await (await fetch("/__test/state")).json()).requests;
  await until(() => $("empty").hidden, "automatic refresh", 18000);
  const after = (await (await fetch("/__test/state")).json()).requests;
  assert(after > before && $("error").hidden, "15-second automatic refresh recovers");
  assert(document.documentElement.scrollWidth <= window.innerWidth, "page fits viewport; only table scrolls horizontally");
  assert(scriptErrors.length === 0, `no uncaught script errors: ${scriptErrors.join("; ")}`);
  document.title = `PASS: ${results.length} dashboard checks`;
  $("test-results").dataset.status = "passed";
} catch (error) {
  results.push(`FAIL ${error.stack || error.message}`);
  $("test-results").textContent = results.join("\n");
  $("test-results").dataset.status = "failed";
  document.title = "FAIL: dashboard checks";
}
