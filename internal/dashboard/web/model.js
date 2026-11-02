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
  return item && ["name", "title", "observer", "family", "impact"].every((key) => typeof item[key] === "string")
    && typeof item.dismissed === "boolean" && /^[a-f0-9]{64}$/.test(item.version)
    && severities.includes(item.severity) && ["open", "resolved"].includes(item.lifecycle?.state)
    && Number.isFinite(Date.parse(item.lifecycle.raised_at)) && Number.isFinite(Date.parse(item.lifecycle.updated_at))
    && isResource(item.anchor) && Array.isArray(item.resources) && item.resources.every(isResource);
}

export function validateSnapshot(data) {
  if (!data || !Number.isFinite(Date.parse(data.fetched_at))
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
        && analysis[key].every((value) => typeof value === "string"))
      || !isEvidence(data?.incident?.evidence)) {
    throw new Error("The dashboard returned invalid incident details.");
  }

  function isEvidence(evidence) {
    if (evidence === undefined) return true;
    if (!evidence || evidence.format !== "kubectl-describe/v1" || !Array.isArray(evidence.descriptions)
        || evidence.descriptions.length < 1 || evidence.descriptions.length > 4
        || !Number.isInteger(evidence.omitted_resources) || evidence.omitted_resources < 0
        || evidence.omitted_resources + evidence.descriptions.length > 50
        || new TextEncoder().encode(JSON.stringify(evidence)).length > 16384) return false;
    const refs = new Set();
    const ids = new Set();
    return evidence.descriptions.every((description) => {
      if (!description || !isResource(description.resource) || typeof description.resource.ref !== "string"
          || !description.resource.ref || typeof description.evidence_id !== "string" || !description.evidence_id
          || description.source !== "kubectl/v0.35.9" || refs.has(description.resource.ref) || ids.has(description.evidence_id)
          || !Number.isFinite(Date.parse(description.observed_at))) return false;
      refs.add(description.resource.ref);
      ids.add(description.evidence_id);
      const section = description.output;
      return section && ["captured", "partial", "unavailable", "absent"].includes(section.status)
        && typeof section.text === "string" && section.text.trim().length > 0
        && new TextEncoder().encode(section.text).length <= 8192;
    });
  }
  return data;
}
