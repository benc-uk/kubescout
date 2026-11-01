export const severities = ["critical", "high", "medium", "low"];
export const clusterScope = "__cluster__";
const ranks = { critical: 4, high: 3, medium: 2, low: 1 };

export function namespaces(issue) {
  return [...new Set([issue.anchor, ...issue.resources].map((r) => r.namespace || clusterScope))];
}

export function selectIssues(items, filters) {
  const search = filters.search.trim().toLowerCase();
  return items.filter((issue) => {
    const state = issue.dismissed ? "dismissed" : issue.lifecycle.state;
    if (filters.state && state !== filters.state) return false;
    if (filters.severity && issue.severity !== filters.severity) return false;
    if (filters.family && issue.family !== filters.family) return false;
    if (filters.namespace && !namespaces(issue).includes(filters.namespace)) return false;
    const text = [issue.name, issue.title, issue.observer,
      ...[issue.anchor, ...issue.resources].flatMap((r) => [r.kind, r.name, r.namespace || "cluster-scoped"])]
      .join(" ").toLowerCase();
    return !search || text.includes(search);
  }).sort((a, b) => {
    let order;
    switch (filters.sort) {
      case "title": order = a.title.localeCompare(b.title); break;
      case "raised": order = Date.parse(a.lifecycle.raised_at) - Date.parse(b.lifecycle.raised_at); break;
      case "updated": order = Date.parse(a.lifecycle.updated_at) - Date.parse(b.lifecycle.updated_at); break;
      default: order = ranks[a.severity] - ranks[b.severity];
    }
    return order * (filters.direction === "asc" ? 1 : -1)
      || Date.parse(b.lifecycle.updated_at) - Date.parse(a.lifecycle.updated_at)
      || a.name.localeCompare(b.name);
  });
}

export function paginate(items, requestedPage, size = 50) {
  const pages = Math.max(1, Math.ceil(items.length / size));
  const page = Math.max(1, Math.min(requestedPage, pages));
  return { page, pages, items: items.slice((page - 1) * size, page * size) };
}

export function severityCounts(items) {
  const counts = Object.fromEntries(severities.map((s) => [s, 0]));
  for (const issue of items) {
    if (issue.lifecycle.state === "open" && !issue.dismissed) counts[issue.severity]++;
  }
  return counts;
}

function isResource(resource) {
  return resource && typeof resource.kind === "string" && typeof resource.name === "string"
    && (resource.namespace === undefined || typeof resource.namespace === "string");
}

function isSummary(item) {
  return item && ["name", "namespace", "title", "observer", "family", "impact"].every((key) => typeof item[key] === "string")
    && typeof item.dismissed === "boolean" && /^[a-f0-9]{64}$/.test(item.version)
    && severities.includes(item.severity) && ["open", "resolved"].includes(item.lifecycle?.state)
    && Number.isFinite(Date.parse(item.lifecycle.raised_at)) && Number.isFinite(Date.parse(item.lifecycle.updated_at))
    && isResource(item.anchor) && Array.isArray(item.resources) && item.resources.every(isResource);
}

export function validateSnapshot(data) {
  if (!data || typeof data.namespace !== "string" || !Number.isFinite(Date.parse(data.fetched_at))
      || !Array.isArray(data.items) || data.items.length > 1000 || !data.items.every(isSummary)) {
    throw new Error("The dashboard returned invalid issue data.");
  }
  return data;
}

export function validateDetail(data) {
  const analysis = data?.item?.analysis;
  if (!data || !Number.isFinite(Date.parse(data.fetched_at)) || !isSummary(data.item)
      || !analysis || !["diagnosis", "explanation", "uncertainty", "model", "prompt_version", "analysis_id"]
        .every((key) => typeof analysis[key] === "string")
      || !["recommendations", "evidence_ids"].every((key) => Array.isArray(analysis[key])
        && analysis[key].every((value) => typeof value === "string"))) {
    throw new Error("The dashboard returned invalid issue details.");
  }
  return data;
}
