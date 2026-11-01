export const severities = ["critical", "high", "medium", "low"];
export const clusterScope = "__cluster__";
const ranks = { critical: 4, high: 3, medium: 2, low: 1 };

export function namespaces(incident) {
  return [...new Set([incident.anchor, ...incident.resources].map((r) => r.namespace || clusterScope))];
}

export function selectIncidents(incidents, filters) {
  const search = filters.search.trim().toLowerCase();
  return incidents.filter((incident) => {
    const state = incident.dismissed ? "dismissed" : incident.lifecycle.state;
    if (filters.state && state !== filters.state) return false;
    if (filters.severity && incident.severity !== filters.severity) return false;
    if (filters.family && incident.family !== filters.family) return false;
    if (filters.namespace && !namespaces(incident).includes(filters.namespace)) return false;
    const text = [incident.name, incident.title, incident.observer,
      ...[incident.anchor, ...incident.resources].flatMap((r) => [r.kind, r.name, r.namespace || "cluster-scoped"])]
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
  for (const incident of items) {
    if (incident.lifecycle.state === "open" && !incident.dismissed) counts[incident.severity]++;
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
      || !Array.isArray(data.incidents) || data.incidents.length > 1000 || !data.incidents.every(isSummary)) {
    throw new Error("The dashboard returned invalid incident data.");
  }
  return data;
}

export function validateDetail(data) {
  const analysis = data?.incident?.analysis;
  if (!data || !Number.isFinite(Date.parse(data.fetched_at)) || !isSummary(data.incident)
      || !analysis || !["diagnosis", "explanation", "uncertainty", "model", "prompt_version", "analysis_id"]
        .every((key) => typeof analysis[key] === "string")
      || !["recommendations", "evidence_ids"].every((key) => Array.isArray(analysis[key])
        && analysis[key].every((value) => typeof value === "string"))) {
    throw new Error("The dashboard returned invalid incident details.");
  }
  return data;
}
