import { Env } from "..";
import { isValidDigest } from "./user";

// Read-only registry endpoints of a repository. Everything else (/v2/, /v2/_catalog, uploads,
// deletes, gc) always needs credentials.
const readPaths: { pattern: RegExp; digest?: boolean }[] = [
  { pattern: /^\/v2\/(.+)\/manifests\/[^/]+$/ },
  { pattern: /^\/v2\/(.+)\/blobs\/([^/]+)$/, digest: true },
  { pattern: /^\/v2\/(.+)\/tags\/list$/ },
  { pattern: /^\/v2\/(.+)\/referrers\/[^/]+$/ },
];

// Turns ANONYMOUS_PULL_REPOSITORIES into regular expressions.
//
// The variable is a comma or whitespace separated list of repository names, `*` matches any
// characters including `/`. Examples: `*` (every repository), `some-org/*`, `some-org/app,other/tool`.
export function anonymousPullPatterns(env: Env): RegExp[] {
  return (env.ANONYMOUS_PULL_REPOSITORIES ?? "")
    .split(/[\s,]+/)
    .filter((p) => p.length > 0)
    .map((p) => new RegExp(`^${p.split("*").map(escapeRegExp).join(".*")}$`));
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Returns the repository name if the request is a pull that may be served without credentials.
export function anonymousPullRepository(env: Env, request: Request): string | null {
  if (request.method !== "GET" && request.method !== "HEAD") {
    return null;
  }

  const patterns = anonymousPullPatterns(env);
  if (patterns.length === 0) {
    return null;
  }

  const path = new URL(request.url).pathname;
  for (const { pattern, digest } of readPaths) {
    const match = pattern.exec(path);
    if (match === null) {
      continue;
    }

    const name = match[1];
    if (digest && !isValidDigest(match[2])) {
      return null;
    }

    return patterns.some((p) => p.test(name)) ? name : null;
  }

  return null;
}
