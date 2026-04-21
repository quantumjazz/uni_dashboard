export function requestedSlug() {
  return window.location.hash.replace(/^#\/?/, "").trim();
}

export function resolveSlug(pages) {
  const requested = requestedSlug();
  if (requested && pages.some((page) => page.slug === requested)) {
    return requested;
  }
  return pages.find((page) => page.status === "active")?.slug || pages[0]?.slug || "";
}

export function ensureRoute(pages) {
  const slug = resolveSlug(pages);
  if (!slug) return "";
  const target = `#/${slug}`;
  if (window.location.hash !== target) {
    window.location.hash = target;
  }
  return slug;
}

export function onRouteChange(callback) {
  window.addEventListener("hashchange", callback);
}
