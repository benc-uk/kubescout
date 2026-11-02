import { selectIncidents, paginate, severityCounts, namespaces, clusterScope, validateSnapshot, validateDetail } from "/model.js";

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
  return [...document.querySelectorAll(".incident-link")].map((node) => node.textContent);
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
  await until(() => $("results").textContent.includes("stored incidents"), "initial render");
  assert($("page-title").textContent === "Incidents", "concise incidents heading");
  assert($("page-title").classList.contains("sr-only") && $("refresh").closest(".connection"),
    "no title banner; refresh sits with snapshot status");
  assert(!document.querySelector(".observation-note"), "no redundant footer disclaimer");
  assert($("counts-title").classList.contains("sr-only") && $("counts-stale").closest(".severity-strip")
    && !$("counts-stale").closest(".sr-only"), "severity heading is hidden visually while stale counts remain visible");
  assert(!$("incident-scope") && !$("namespace"), "no redundant incident scope or storage namespace label");
  const overviewNodes = [document.querySelector(".counts"), $("freshness"), $("refresh")];
  const overviewCenters = overviewNodes.map((node) => {
    const rect = node.getBoundingClientRect();
    return rect.top + rect.height / 2;
  });
  assert(overviewNodes.every((node) => node.closest(".connection"))
    && Math.max(...overviewCenters) - Math.min(...overviewCenters) <= 2
    && document.querySelector(".connection").getBoundingClientRect().height <= 80,
    "severity counts, freshness and refresh share one compact desktop bar");
  document.querySelector('[data-severity="low"]').click();
  assert($("severity").value === "low" && $("state").value === "open"
    && document.querySelector('[data-severity="low"]').getAttribute("aria-pressed") === "true",
    "inline severity counts still filter open incidents");
  document.querySelector('[data-severity="low"]').click();
  assert($("severity").value === "", "activating the same severity clears the filter");
  const logo = document.querySelector(".brand-logo");
  await until(() => logo.complete && logo.naturalWidth > 0, "brand logo loads");
  assert(document.querySelector('link[rel="icon"]').getAttribute("href") === logo.getAttribute("src"),
    "header and favicon use the local logo");
  assert(getComputedStyle(document.querySelector(".counts")).display === "flex"
    && getComputedStyle(document.querySelector(".incidents-panel")).boxShadow === "none",
    "compact severity strip and flat incident table");
  assert(document.querySelectorAll("thead th").length === 5 && document.querySelector("#incident-rows tr").children.length === 5,
    "no unused chevron column");
  assert(parseFloat(getComputedStyle(document.querySelector(".incident-link")).fontSize) >= 20
    && parseFloat(getComputedStyle($("search")).fontSize) >= 18
    && parseFloat(getComputedStyle(document.querySelector(".incident-meta")).fontSize) >= 15,
    "larger incident, control and secondary text");
  assert([...document.querySelectorAll(".badge, .count-label")].every((node) =>
    parseFloat(getComputedStyle(node).fontSize) >= 18),
    "severity and state labels are at least 18px");
  const previousTheme = localStorage.getItem("kubescout-dashboard-theme");
  const initialDark = $("theme-toggle").getAttribute("aria-pressed") === "true";
  for (const theme of ["light", "dark"]) {
    if (($("theme-toggle").getAttribute("aria-pressed") === "true") !== (theme === "dark")) $("theme-toggle").click();
    assert(getComputedStyle(document.documentElement).colorScheme.includes(theme), `${theme} theme applies to native controls`);
    const readable = [...document.querySelectorAll(".count-card, .badge, .connection, th, .incident-link, .incident-meta, #theme-toggle, #direction, #error")]
      .every((node) => contrast(node) >= 4.5);
    assert(readable, `${theme} theme text contrast is at least 4.5:1`);
  }
  assert(localStorage.getItem("kubescout-dashboard-theme") === "dark", "explicit theme choice is saved");
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
  if (previousTheme === null) localStorage.removeItem("kubescout-dashboard-theme");
  else localStorage.setItem("kubescout-dashboard-theme", previousTheme);
  const rawSnapshot = await (await fetch("/api/incidents")).json();
  assert(Array.isArray(rawSnapshot.incidents) && !("items" in rawSnapshot), "incident API returns an incidents collection");
  assert(!("namespace" in rawSnapshot) && rawSnapshot.incidents.every((item) => !("namespace" in item)),
    "cluster-scoped incident API omits storage namespaces");
  const data = validateSnapshot(rawSnapshot);
  const defaults = { search: "", state: "open", severity: "", family: "", namespace: "", sort: "severity", direction: "desc" };
  const selected = selectIncidents(data.incidents, defaults);
  assert(selected.length === 60 && selected[0].name === "incident-056", "open-only default with severity and newest-first tie-breaker");
  assert(selectIncidents(data.incidents, { ...defaults, direction: "asc" })[0].severity === "low", "numeric ascending severity");
  assert(selectIncidents(data.incidents, { ...defaults, sort: "title", direction: "asc" })[0].name === "incident-000", "title sorting");
  assert(selectIncidents(data.incidents, { ...defaults, sort: "updated" })[0].name === "incident-059", "updated sorting");
  assert(selectIncidents(data.incidents, { ...defaults, state: "resolved" }).length === 4, "resolved filtering");
  const stateCases = [
    { ...data.incidents[0], name: "open", dismissed: false },
    { ...data.incidents[60], name: "resolved", dismissed: false },
    { ...data.incidents[0], name: "dismissed-open", dismissed: true },
    { ...data.incidents[60], name: "dismissed-resolved", dismissed: true },
  ];
  assert(selectIncidents(stateCases, defaults).map((item) => item.name).join() === "open"
    && selectIncidents(stateCases, { ...defaults, state: "resolved" }).map((item) => item.name).join() === "resolved"
    && selectIncidents(stateCases, { ...defaults, state: "dismissed" }).length === 2
    && selectIncidents(stateCases, { ...defaults, state: "" }).length === 4,
    "one state filter separates dismissed incidents and includes everything in all states");
  assert(selectIncidents(data.incidents, { ...defaults, search: "OTHER-OBSERVER", namespace: "payments", severity: "medium" }).length === 1, "combined case-insensitive search and filters");
  const multiNamespaceIncident = { ...data.incidents[0], anchor: { kind: "Deployment", namespace: "apps", name: "checkout" },
    resources: [{ kind: "Pod", namespace: "payments", name: "worker" }] };
  assert(selectIncidents([multiNamespaceIncident], { ...defaults, namespace: "payments" }).length === 1
    && selectIncidents([multiNamespaceIncident], { ...defaults, namespace: "apps" }).length === 1
    && selectIncidents([multiNamespaceIncident], { ...defaults, namespace: "unrelated" }).length === 0,
    "namespace filter matches anchors and affected workloads across namespaces");
  assert(namespaces(data.incidents[1]).includes(clusterScope), "cluster-scoped resource filter");
  assert(paginate(selected, 1).items.length === 50 && paginate(selected, 999).page === 2, "50-row pagination and clamping");
  assert(severityCounts(data.incidents).critical === 15, "counts exclude resolved incidents");
  assert(document.querySelectorAll(".sort-controls select").length === 1
    && $("direction").tagName === "BUTTON" && $("direction").type === "button",
    "sorting uses one selector and a reverse-order button");
  const sortBounds = document.querySelector(".sort-controls").getBoundingClientRect();
  const directionBounds = $("direction").getBoundingClientRect();
  assert(sortBounds.width <= 300 && directionBounds.width >= 44 && directionBounds.height >= 44,
    "compact sorting group retains a 44px direction target");
  assert([...document.querySelectorAll("select")].every((node) =>
    getComputedStyle(node).appearance === "none" && parseFloat(getComputedStyle(node).paddingRight) >= 40),
    "all dropdowns reserve space between text and chevron");
  const sortArrow = getComputedStyle(document.querySelector(".sort-inputs"), "::after");
  assert(parseFloat(sortArrow.right) - directionBounds.width >= 16 && sortArrow.pointerEvents === "none"
    && [...document.querySelectorAll(".filters label:has(select)")].every((node) =>
      parseFloat(getComputedStyle(node, "::after").right) >= 16),
    "dropdown chevrons sit at least 16px inside the edge without intercepting clicks");
  assert($("direction").value === "desc" && $("direction").getAttribute("aria-label").includes("Highest severity first")
    && $("direction").title.includes("Switch to lowest severity first"), "default direction explains current order and action");
  $("next").click();
  $("direction").focus();
  $("direction").click();
  assert($("direction").value === "asc" && document.querySelector("#incident-rows .badge").textContent === "Low"
    && $("page-info").textContent.includes("Page 1") && document.activeElement === $("direction"),
    "reversing severity resets pagination and preserves button focus");
  assert($("direction").getAttribute("aria-label").includes("Lowest severity first")
    && getComputedStyle($("direction").querySelector("svg")).transform !== "none",
    "ascending order updates the accessible description and arrow");
  $("direction").click();
  assert($("direction").value === "desc" && document.querySelector(".incident-link").dataset.incident === "incident-056"
    && getComputedStyle($("direction").querySelector("svg")).transform === "none",
    "reversing again restores descending severity and arrow");
  for (const [sort, direction, description] of [
    ["title", "asc", "Title A to Z"], ["updated", "desc", "Newest update first"],
    ["raised", "desc", "Newest incident first"],
  ]) {
    set("sort", sort);
    assert($("direction").value === direction && $("direction").getAttribute("aria-label").startsWith(description),
      `${sort} selection keeps its default direction with a meaningful description`);
    for (let i = 0; i < 2; i++) {
      const expected = paginate(selectIncidents(data.incidents, { ...defaults, sort, direction: $("direction").value }), 1);
      assert([...document.querySelectorAll(".incident-link")].map((node) => node.dataset.incident).join()
        === expected.items.map((item) => item.name).join(), `${sort} ${$("direction").value} button orders the displayed rows`);
      $("direction").click();
    }
  }
  set("sort", "severity");
  let rejected = false;
  try { validateSnapshot({ incidents: null }); } catch { rejected = true; }
  assert(rejected, "malformed snapshot rejected");
  const rawDetail = await (await fetch("/api/incidents/incident-000")).json();
  assert(rawDetail.incident && !("item" in rawDetail), "incident detail API uses an incident property");
  validateDetail(rawDetail);
  assert(Array.isArray(rawDetail.incident.evidence.descriptions) && !("snapshots" in rawDetail.incident.evidence),
    "incident evidence uses descriptions without the previous field");
  const obsoleteDetail = structuredClone(rawDetail);
  obsoleteDetail.incident.evidence.snapshots = obsoleteDetail.incident.evidence.descriptions;
  delete obsoleteDetail.incident.evidence.descriptions;
  rejected = false;
  try { validateDetail(obsoleteDetail); } catch { rejected = true; }
  assert(rejected, "previous resource evidence field is not accepted as a descriptions alias");
  assert(document.querySelectorAll(".incident-link").length === 50 && $("next").disabled === false, "table starts on first page");
  $("next").click();
  assert(document.querySelectorAll(".incident-link").length === 10 && $("page-info").textContent.includes("Page 2"), "next page shows remaining rows");
  set("severity", "critical");
  assert(titles().length === 15 && $("page-info").textContent.includes("Page 1"), "filter resets page");
  set("family", "node-health");
  assert(!$("empty").hidden && $("empty-title").textContent === "No matching incidents", "combined filters and empty state");
  $("reset-filters").click();
  set("family", "service-availability");
  assert(titles().length === 1 && titles()[0].includes("03"), "service availability family filter");
  $("reset-filters").click();
  set("workload-namespace", "payments");
  assert(titles().length === selectIncidents(data.incidents, { ...defaults, namespace: "payments" }).length,
    "namespace dropdown filters workloads");
  $("reset-filters").click();
  set("workload-namespace", clusterScope);
  assert(titles().length === 1 && titles()[0].includes("01"), "workload namespace distinguishes cluster-scoped resources");
  $("reset-filters").click();
  set("state", "resolved");
  assert(titles().length === 4 && document.querySelectorAll(".badge.resolved").length === 4, "resolved UI view");
  $("reset-filters").click();
  set("search", "onerror");
  assert(titles().length === 1 && !document.querySelector("#incident-rows img"), "untrusted title rendered as text");
  document.querySelector(".incident-link").click();
  await until(() => $("detail-content").textContent.includes("Recommendations"), "details loaded");
  const modalBounds = $("detail").getBoundingClientRect();
  assert(Math.abs(modalBounds.width - Math.min(1000, innerWidth - 64)) <= 2
    && Math.abs(modalBounds.left - (innerWidth - modalBounds.width) / 2) <= 2
    && Math.abs(modalBounds.top - (innerHeight - modalBounds.height) / 2) <= 2,
    "Focused review is a large centered desktop modal, not a side drawer");
  assert(getComputedStyle($("detail-body")).overflowY === "auto"
    && getComputedStyle(document.body).overflow === "hidden"
    && $("detail").scrollHeight === $("detail").clientHeight,
    "only the modal body scrolls; header, footer and background stay in place");
  assert($("detail-tab-overview").getAttribute("aria-selected") === "true"
    && !$("detail-overview").hidden && $("detail-resources").hidden && $("detail-record").hidden,
    "new incident opens on Overview with one visible panel");
  assert($("detail-tabs").querySelectorAll('[role="tab"]').length === 3
    && ["overview", "resources", "record"].every((name) =>
      $(`detail-tab-${name}`).getAttribute("aria-controls") === `detail-${name}`
      && $(`detail-${name}`).getAttribute("aria-labelledby") === `detail-tab-${name}`),
    "tabs and panels have accessible names and paired ARIA relationships");
  assert(getComputedStyle(document.querySelector(".detail-overview-grid")).gridTemplateColumns.split(" ").length === 2,
    "Overview keeps explanation and recommendations side by side on desktop");
  const headerTop = $("detail").querySelector(".detail-header").getBoundingClientRect().top;
  const footerTop = $("detail").querySelector(".detail-footer").getBoundingClientRect().top;
  $("detail-body").scrollTop = 500;
  assert($("detail-body").scrollTop > 0
    && $("detail").querySelector(".detail-header").getBoundingClientRect().top === headerTop
    && $("detail").querySelector(".detail-footer").getBoundingClientRect().top === footerTop,
    "long untrusted conclusions scroll without moving the header or triage footer");
  $("detail-tab-overview").focus();
  $("detail-tab-overview").dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true, cancelable: true }));
  assert(document.activeElement === $("detail-tab-resources") && !$("detail-resources").hidden
    && $("detail-overview").hidden && $("detail-body").scrollTop === 0
    && $("detail-resource-count").textContent === String(rawDetail.incident.resources.length),
    "Right arrow selects Resources, moves focus and resets the body scroll");
  $("detail-tab-resources").dispatchEvent(new KeyboardEvent("keydown", { key: "End", bubbles: true, cancelable: true }));
  assert(document.activeElement === $("detail-tab-record") && !$("detail-record").hidden
    && $("detail-tab-record").tabIndex === 0 && $("detail-tab-resources").tabIndex === -1,
    "End selects Record with a single tab stop");
  $("detail-tab-record").dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true, cancelable: true }));
  assert(document.activeElement === $("detail-tab-overview"), "Right arrow wraps from Record to Overview");
  $("detail-tab-overview").dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowLeft", bubbles: true, cancelable: true }));
  assert(document.activeElement === $("detail-tab-record"), "Left arrow wraps from Overview to Record");
  $("detail-tab-record").dispatchEvent(new KeyboardEvent("keydown", { key: "Home", bubbles: true, cancelable: true }));
  assert(document.activeElement === $("detail-tab-overview"), "Home returns to Overview");
  $("detail-tab-record").click();
  const problemNamespace = [...$("detail-content").querySelectorAll("dt")].find((node) => node.textContent === "Problem namespace");
  assert(problemNamespace?.nextElementSibling?.textContent === "apps"
    && !$("detail-content").textContent.includes("Incident namespace"), "details show problem namespace instead of incident-storage namespace");
  const resourceName = [...$("detail-content").querySelectorAll("dt")].find((node) =>
    node.textContent === "Resource name").nextElementSibling;
  const namespaceValue = problemNamespace.nextElementSibling;
  assert(resourceName.querySelector("code").textContent === data.incidents[0].anchor.name
    && namespaceValue.querySelector("code").textContent === "apps"
    && !$("incident-rows").querySelector(".copy-button") && !document.querySelector(".connection .copy-button"),
    "exact resource names and namespaces are copyable only in the details view");
  const affectedResources = [...$("detail-content").querySelectorAll(".detail-section")].find((node) =>
    node.querySelector("h3")?.textContent === "Affected resources");
  assert(!affectedResources.querySelector(".copy-button") && affectedResources.querySelector("li").textContent
    === `${data.incidents[0].anchor.kind} / ${data.incidents[0].anchor.namespace}/${data.incidents[0].anchor.name}`,
    "affected resources remain plain text without copy controls");
  const clipboardDescriptor = Object.getOwnPropertyDescriptor(navigator, "clipboard");
  const copied = [];
  const clipboard = { writeText: async (value) => { copied.push(value); } };
  try {
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: clipboard });
    resourceName.querySelector(".copy-button").click();
    await until(() => resourceName.querySelector(".copy-feedback").textContent === "Copied", "resource name copied");
    namespaceValue.querySelector(".copy-button").click();
    await until(() => namespaceValue.querySelector(".copy-feedback").textContent === "Copied", "namespace copied");
    assert(copied.join() === `${data.incidents[0].anchor.name},apps`,
      "clipboard receives only identifiers, without kind prefixes, slashes or UI labels");
    clipboard.writeText = async () => { throw new DOMException("Fixture clipboard denial", "NotAllowedError"); };
    namespaceValue.querySelector(".copy-button").click();
    await until(() => namespaceValue.querySelector(".copy-feedback").textContent.includes("Browser blocked copying"),
      "clipboard denial reported");
    assert(document.getSelection().toString() === "apps"
      && !namespaceValue.querySelector(".copy-button").disabled,
      "clipboard denial selects the exact value for manual copying and restores the control");
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: undefined });
    resourceName.querySelector(".copy-button").click();
    assert(resourceName.querySelector(".copy-feedback").textContent.includes("Clipboard unavailable")
      && document.getSelection().toString() === data.incidents[0].anchor.name,
      "unavailable clipboard explicitly offers manual copying instead of claiming success");
  } finally {
    if (clipboardDescriptor) Object.defineProperty(navigator, "clipboard", clipboardDescriptor);
    else delete navigator.clipboard;
    document.getSelection().removeAllRanges();
  }
  assert(parseFloat(getComputedStyle(document.querySelector(".detail-section p")).fontSize) >= 16, "larger detail text");
  assert($("detail").open && !$("detail").querySelector("img, script") && !window.injected, "safe drill-down and untrusted title and recommendations");
  assert($("detail-content").textContent.includes("Evidence IDs") && $("detail-content").textContent.includes("Revision"), "evidence and lifecycle details");
  assert($("detail-resources").textContent.includes("Resource descriptions")
    && $("detail-resources").textContent.includes("2 additional context resources omitted")
    && $("detail-resources").textContent.includes("Partial")
    && $("detail-resources").textContent.includes("Unavailable"),
    "stored descriptions display coverage and unavailable sections");
  assert($("detail-resources").textContent.includes("<img src=x onerror=alert(1)>")
    && !document.querySelector(".description-entry img")
    && document.querySelector('.description-entry button[aria-label="Copy kubectl-describe-1 kubectl describe"]'),
    "description text is safely rendered and copyable");
  $("detail-tab-resources").click();
  const descriptionEntries = [...document.querySelectorAll(".description-entry")];
  assert(descriptionEntries.length === 3 && descriptionEntries.every((entry) => entry.tagName === "DETAILS" && !entry.open),
    "each resource starts collapsed instead of exposing one long evidence list");
  assert(descriptionEntries.every((entry) => entry.querySelector("summary .description-name")
    && entry.querySelector("summary .description-kind") && entry.querySelectorAll("summary .description-status").length === 1)
    && descriptionEntries[2].querySelector("summary").textContent.includes("Cluster-scoped"),
    "collapsed summaries show resource identity, namespace or cluster scope, and section coverage");
  const firstDescription = descriptionEntries[0];
  firstDescription.querySelector("summary").click();
  assert(firstDescription.open && descriptionEntries.slice(1).every((entry) => !entry.open),
    "a resource expands independently");
  const storedText = rawDetail.incident.evidence.descriptions[0].output.text;
  const descriptionOutput = firstDescription.querySelector(".copy-value");
  assert(descriptionOutput.textContent === storedText
    && descriptionOutput.textContent.includes("Ready  False")
    && getComputedStyle(descriptionOutput).whiteSpace === "pre-wrap"
    && getComputedStyle(descriptionOutput).overflowY === "auto"
    && parseFloat(getComputedStyle(descriptionOutput).lineHeight) >= 22,
    "native text preserves indentation, readable spacing and bounded scrolling");
  const nativeDescribe = firstDescription.querySelector(".native-describe");
  assert(nativeDescribe.textContent === storedText
    && getComputedStyle(nativeDescribe).tabSize === "8"
    && firstDescription.querySelector("summary").textContent.includes("Describe: Partial")
    && firstDescription.textContent.includes("kubectl describe")
    && firstDescription.textContent.includes("kubectl-describe-1")
    && firstDescription.querySelectorAll(".description-panel").length === 1
    && !firstDescription.textContent.includes("Resource state")
    && !firstDescription.textContent.includes("Warning events"),
    "each resource shows only one native describe block, coverage and citation");
  assert(Object.keys(rawDetail.incident.evidence.descriptions[0]).sort().join() === "evidence_id,observed_at,output,resource,source",
    "Incident payload contains native describe only, without typed state or event copies");
  const descriptionClipboardDescriptor = Object.getOwnPropertyDescriptor(navigator, "clipboard");
  let copiedDescription;
  try {
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: {
      writeText: async (text) => { copiedDescription = text; },
    } });
    firstDescription.querySelector(".copy-button").click();
    await until(() => copiedDescription !== undefined && !firstDescription.querySelector(".copy-button").disabled, "description copied");
    assert(copiedDescription === storedText && copiedDescription === nativeDescribe.textContent,
      "native describe copy matches the sole stored resource description");
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: undefined });
    firstDescription.querySelector(".copy-button").click();
    assert(document.getSelection().toString() === storedText && descriptionOutput.textContent === storedText
      && firstDescription.querySelector(".copy-feedback").textContent.includes("Clipboard unavailable"),
      "manual description copying reveals and selects the original stored text");
  } finally {
    if (descriptionClipboardDescriptor) Object.defineProperty(navigator, "clipboard", descriptionClipboardDescriptor);
    else delete navigator.clipboard;
    document.getSelection().removeAllRanges();
  }
  document.querySelector('[aria-label="Expand all resource descriptions"]').click();
  assert(descriptionEntries.every((entry) => entry.open), "Expand all opens every resource");
  document.querySelector('[aria-label="Collapse all resource descriptions"]').click();
  assert(descriptionEntries.every((entry) => !entry.open), "Collapse all closes every resource");
  firstDescription.querySelector("summary").click();
  firstDescription.querySelector("summary").focus();
  await refresh();
  await until(() => !document.contains(firstDescription), "resource descriptions refreshed");
  assert(document.querySelector(".description-entry").open
    && document.querySelectorAll(".description-entry[open]").length === 1
    && document.activeElement === document.querySelector(".description-entry summary"),
    "refresh preserves expanded resources and disclosure keyboard focus");
  const readingEntries = [...document.querySelectorAll(".description-entry")];
  readingEntries[1].querySelector("summary").click();
  const readingOutputs = readingEntries.slice(0, 2).map((entry) => entry.querySelector(".native-describe"));
  readingOutputs[1].focus({ preventScroll: true });
  readingOutputs[0].scrollTop = 180;
  readingOutputs[1].scrollTop = 240;
  $("detail-body").scrollTop = 400;
  const readingScroll = $("detail-body").scrollTop;
  assert(readingOutputs[0].scrollTop === 180 && readingOutputs[1].scrollTop === 240 && readingScroll > 0,
    "long native fixtures exercise independent output and modal scrolling");
  await refresh();
  await until(() => !document.contains(readingEntries[0]), "scrolled native outputs refreshed");
  const refreshedOutputs = [...document.querySelectorAll(".native-describe")];
  assert(Math.abs(refreshedOutputs[0].scrollTop - 180) <= 2
    && Math.abs(refreshedOutputs[1].scrollTop - 240) <= 2
    && Math.abs($("detail-body").scrollTop - readingScroll) <= 2
    && document.activeElement === refreshedOutputs[1],
    "refresh preserves each describe reading position, output keyboard focus and modal scroll");
  const beforeRecord = $("detail-record").firstElementChild;
  $("detail-tab-record").click();
  $("detail-tab-record").focus();
  $("detail-body").scrollTop = 100;
  const recordScroll = $("detail-body").scrollTop;
  await refresh();
  await until(() => $("detail-record").firstElementChild !== beforeRecord, "record refreshed");
  assert($("search").value === "onerror" && $("detail").open
    && $("detail-tab-record").getAttribute("aria-selected") === "true"
    && !$("detail-record").hidden && document.activeElement === $("detail-tab-record")
    && Math.abs($("detail-body").scrollTop - recordScroll) <= 2,
    "refresh preserves selection, filters, active tab, keyboard focus and body scroll");
  const copyBeforeRefresh = $("detail-record").querySelector(".copy-button");
  copyBeforeRefresh.focus({ preventScroll: true });
  await refresh();
  await until(() => !$("detail-record").contains(copyBeforeRefresh), "copy controls refreshed");
  assert(document.activeElement?.getAttribute("aria-label") === copyBeforeRefresh.getAttribute("aria-label"),
    "refresh restores focus to the same copy control in Record");
  await state("error");
  await refresh();
  await until(() => !$("detail-error").hidden, "detail failure");
  assert(!$("error").hidden && $("error").textContent.includes("stale") && titles().length === 1, "failed refresh keeps stale rows");
  assert(!$("counts-stale").hidden && $("freshness").textContent.includes("Stale"),
    "combined overview visibly marks both counts and snapshot status stale");
  assert($("detail-error").textContent.includes("stale") && $("detail-content").textContent.includes("Recommendations")
    && !$("detail-record").hidden && $("triage-action").disabled, "failed detail refresh keeps the selected tab and stale content but disables triage");
  await state("deleted");
  await refresh();
  await until(() => $("detail-freshness").textContent === "Incident unavailable", "deleted detail");
  assert($("detail-content").textContent.trim() === "" && $("detail-content").hidden
    && $("detail-tabs").hidden && $("detail-tags").hidden && $("detail-resource").hidden
    && $("detail-resource-count").textContent === "" && $("triage-action").hidden,
    "deleted incident clears all panels, header context and tabs rather than showing current data");
  $("close-detail").click();
  await until(() => document.activeElement?.className === "incident-link", "focus restoration");
  assert(!$("detail").open, "detail closes and restores focus");
  await state("slow");
  $("reset-filters").click();
  document.querySelector(".incident-link").click();
  assert(!$("detail-loading").hidden && $("detail-tabs").hidden && $("detail-content").hidden
    && $("triage-action").hidden, "loading a new incident never exposes old panels, tabs or triage");
  $("close-detail").click();
  await until(() => !$("detail").open, "old detail closed");
  set("search", "Workload 01");
  await state("normal");
  document.querySelector(".incident-link").click();
  await until(() => $("detail-content").textContent.includes("worker-1"), "new selection");
  assert($("detail-tab-overview").getAttribute("aria-selected") === "true" && $("detail-record").hidden,
    "new selection resets the previous Record tab to Overview");
  assert(!document.querySelector(".description-entry[open]"), "a different incident does not inherit expanded resources");
  const nodeNamespace = [...$("detail-content").querySelectorAll("dt")].find((node) => node.textContent === "Problem namespace");
  assert(nodeNamespace?.nextElementSibling?.textContent === "Cluster-scoped", "node problems display cluster scope rather than incident-storage namespace");
  await sleep(600);
  assert($("detail-content").textContent.includes("worker-1"), "obsolete detail response cannot replace newer selection");
  $("close-detail-footer").click();
  await until(() => !$("detail").open, "footer closes modal");
  await state("error");
  document.querySelector(".incident-link").click();
  await until(() => !$("detail-error").hidden, "initial detail error");
  assert($("detail-freshness").textContent === "No successful detail refresh"
    && $("detail-tabs").hidden && $("detail-content").hidden && $("detail-loading").hidden && $("triage-action").hidden,
    "initial detail error is explicit without showing blank tabs or stale data");
  $("close-detail").click();
  await state("normal");
  $("reset-filters").click();
  set("search", "Workload 04");
  document.querySelector(".incident-link").click();
  await until(() => !$("detail-content").hidden, "empty detail lists");
  assert($("detail-overview").textContent.includes("None recorded") && $("detail-resource-count").textContent === "0",
    "empty recommendations and zero resource count remain explicit");
  $("detail-tab-resources").click();
  assert($("detail-resources").textContent.includes("None recorded"), "Resources handles an empty affected-resource list");
  $("detail-tab-record").click();
  assert($("detail-record").textContent.includes("None recorded"), "Record handles empty evidence references");
  $("close-detail").click();
  $("reset-filters").click();
  set("search", "onerror");
  document.querySelector(".incident-link").click();
  await until(() => !$("triage-action").hidden && !$("triage-action").disabled, "dismiss available");
  $("triage-action").click();
  await until(() => $("triage-action").textContent === "Restore incident" && !$("triage-action").disabled
    && titles().length === 0, "dismissal saved and default list refreshed");
  assert($("detail-tags").querySelector(".badge.dismissed")
    && !$("detail-tags").querySelector(".badge.open, .badge.resolved") && $("count-critical").textContent === "14",
    "dismissed replaces lifecycle badge and is excluded from open severity counts");
  await refresh();
  await until(() => !$("triage-action").disabled, "dismissal survives refresh");
  assert($("triage-action").textContent === "Restore incident", "unchanged refresh retains dismissal");
  $("close-detail").click();
  set("state", "dismissed");
  assert(titles().length === 1, "dismissed filter exposes hidden incident");
  assert($("incident-rows").querySelectorAll("td:nth-child(3) .badge").length === 1
    && $("incident-rows").querySelector("td:nth-child(3) .badge.dismissed"),
    "dismissed row shows only its effective state");
  set("state", "");
  assert(!$("incident-rows").querySelector(".badge.open, .badge.resolved"),
    "all states view also shows only dismissed for the dismissed incident");
  set("state", "dismissed");
  document.querySelector(".incident-link").click();
  await until(() => !$("triage-action").disabled, "restore available");
  $("triage-action").click();
  await until(() => $("triage-action").textContent === "Dismiss incident" && !$("triage-action").disabled
    && titles().length === 0, "restore saved");
  $("close-detail").click();
  set("state", "open");
  assert(titles().length === 1, "restored incident returns to default view");
  assert($("incident-rows").querySelector("td:nth-child(3) .badge.open")
    && !$("incident-rows").querySelector(".badge.dismissed"), "restore displays the underlying lifecycle state");
  document.querySelector(".incident-link").click();
  await until(() => !$("triage-action").disabled, "dismiss ready again");
  await state("error");
  $("triage-action").click();
  await until(() => !$("triage-error").hidden, "write failure reported");
  assert($("triage-error").textContent.includes("denied"), "write denial is visible, not optimistic dismissal");
  await state("normal");
  await refresh();
  await until(() => !$("triage-action").disabled, "recovered detail");
  const previousRevision = [...$("detail-content").querySelectorAll("dt")].find((node) =>
    node.textContent === "Revision").nextElementSibling.textContent;
  await state("update");
  $("triage-action").click();
  await until(() => !$("triage-error").hidden && $("triage-error").textContent.includes("changed"), "stale detail rejected");
  await until(() => !$("triage-action").disabled
    && [...$("detail-content").querySelectorAll("dt")].find((node) =>
      node.textContent === "Revision").nextElementSibling.textContent !== previousRevision, "fresh detail after conflict");
  assert($("triage-action").textContent === "Dismiss incident", "conflict does not hide the newer revision");
  $("detail-tab-resources").click();
  const revisionEntry = document.querySelector(".description-entry");
  revisionEntry.querySelector("summary").click();
  const revisionOutput = revisionEntry.querySelector(".native-describe");
  revisionOutput.scrollTop = 160;
  await state("update");
  await refresh();
  await until(() => !document.contains(revisionEntry), "new revision descriptions refreshed");
  assert(!document.querySelector(".description-entry[open]")
    && document.querySelector(".native-describe").scrollTop === 0,
    "material revision resets expansion and describe scroll rather than reusing the old reading position");
  await until(() => !$("triage-action").disabled, "new revision ready to dismiss");
  $("triage-action").click();
  await until(() => $("triage-action").textContent === "Restore incident" && !$("triage-action").disabled, "new revision dismissed");
  await state("update");
  await refresh();
  await until(() => $("triage-action").textContent === "Dismiss incident" && titles().length === 1, "material update reappears");
  assert($("count-critical").textContent === "15", "updated incident returns to open severity counts");
  $("close-detail").click();
  $("reset-filters").click();
  await state("empty");
  await refresh();
  assert(!$("empty").hidden && $("empty-title").textContent === "No stored incidents", "empty snapshot differs from failure");
  await state("normal");
  const before = (await (await fetch("/__test/state")).json()).requests;
  await until(() => $("empty").hidden, "automatic refresh", 18000);
  const after = (await (await fetch("/__test/state")).json()).requests;
  assert(after > before && $("error").hidden, "15-second automatic refresh recovers");
  set("search", "onerror");
  document.querySelector(".incident-link").click();
  await until(() => !$("detail-content").hidden, "automatic refresh reading fixture");
  $("detail-tab-resources").click();
  const automaticEntry = document.querySelector(".description-entry");
  automaticEntry.querySelector("summary").click();
  const automaticOutput = automaticEntry.querySelector(".native-describe");
  automaticOutput.focus({ preventScroll: true });
  automaticOutput.scrollTop = 220;
  $("detail-body").scrollTop = 300;
  const automaticScroll = $("detail-body").scrollTop;
  await until(() => !document.contains(automaticEntry), "automatic description refresh", 18000);
  const automaticReplacement = document.querySelector(".native-describe");
  assert(Math.abs(automaticReplacement.scrollTop - 220) <= 2
    && Math.abs($("detail-body").scrollTop - automaticScroll) <= 2
    && document.activeElement === automaticReplacement,
    "15-second automatic refresh preserves describe scroll and reading focus");
  $("close-detail").click();
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
