/**
 * The core server that runs on a Cloudflare worker.
 */

import { Router } from "itty-router";
import { AuthErrorResponse, InternalError } from "./src/errors";
import v2Router from "./src/router";
import { authenticationMethodFromEnv } from "./src/authentication-method";
import { Registry } from "./src/registry/registry";
import { R2Registry } from "./src/registry/r2";
import { RegionalCache } from "./src/registry/regional-cache";
import { anonymousPullRepository } from "./src/anonymous";

// A full compatibility mode means that the r2 registry will try its best to
// help the client on the layer push. See how we let the client push layers with chunked uploads for more information.
type PushCompatibilityMode = "full" | "none";

export interface Env {
  REGISTRY: R2Bucket;
  // Regional read-through caches in front of REGISTRY (see src/registry/regional-cache.ts)
  REGISTRY_CACHE_EU?: R2Bucket;
  REGISTRY_CACHE_US?: R2Bucket;
  // Objects up to this size fill the cache via tee(), bigger ones via a second read. Default 64 MiB.
  CACHE_TEE_MAX_BYTES?: string;
  ENVIRONMENT: string;
  JWT_REGISTRY_TOKENS_PUBLIC_KEY?: string;
  USERNAME?: string;
  PASSWORD?: string;
  READONLY_USERNAME?: string;
  READONLY_PASSWORD?: string;
  PUSH_COMPATIBILITY_MODE?: PushCompatibilityMode;
  REGISTRIES_JSON?: string; // should be in the format of RegistryConfiguration[];
  // Repositories that can be pulled without credentials, see src/anonymous.ts
  ANONYMOUS_PULL_REPOSITORIES?: string;
  // Per request, set by fetch() below
  REGISTRY_CLIENT: Registry;
  ANONYMOUS_REQUEST?: boolean;
}

const router = Router();

/**
 * V2 Api
 */
router.all("/v2/*", v2Router.fetch);

router.all("*", () => new Response("Not Found.", { status: 404 }));

export default {
  async fetch(request: Request, env: Env, context?: ExecutionContext) {
    if (!ensureConfig(env)) {
      return new AuthErrorResponse(request);
    }

    const authMethod = await authenticationMethodFromEnv(env);
    if (!authMethod) {
      return new AuthErrorResponse(request);
    }

    let anonymous = false;
    const credentials = await authMethod.checkCredentials(request);
    if (!credentials.verified) {
      // Requests without any credentials may pull repositories listed in ANONYMOUS_PULL_REPOSITORIES.
      // Wrong or expired credentials are still rejected, so clients notice them. /v2/ keeps answering
      // 401 with a Basic challenge, which is what makes docker send credentials for pushes.
      anonymous = request.headers.get("Authorization") === null && anonymousPullRepository(env, request) !== null;
      if (!anonymous) {
        console.warn(`Not Authorized. authmode=${authMethod.authmode}. verified=false`);
        return new AuthErrorResponse(request);
      }
    }

    // env is shared by all concurrent requests of this isolate, so everything that depends on the
    // request (regional cache, execution context, anonymous) goes into a copy.
    const requestEnv: Env = { ...env, ANONYMOUS_REQUEST: anonymous };
    // Only reads are served from the regional cache, pushes always go to the primary bucket
    const readOnly = request.method === "GET" || request.method === "HEAD";
    requestEnv.REGISTRY_CLIENT = new R2Registry(
      requestEnv,
      readOnly ? RegionalCache.fromRequest(requestEnv, request, context) : undefined,
    );
    try {
      // Dispatch the request to the appropriate route
      const res = await router.fetch(request, requestEnv, context);
      return res;
    } catch (err) {
      if (err instanceof Response) {
        console.warn(`${request.method} ${err.status} ${err.url}`);
        return err;
      }

      // Unexpected error
      if (err instanceof Error) {
        console.error(
          "An error has been thrown by the router:\n",
          `${err.name}: ${err.message}: ${err.cause}: ${err.stack}`,
        );
        return new InternalError();
      }

      console.error(
        "An error has been thrown and is neither a Response or an Error, JSON.stringify() =",
        JSON.stringify(err),
      );
      return new InternalError();
    }
  },
} satisfies ExportedHandler<Env>;

const ensureConfig = (env: Env): boolean => {
  if (!env.REGISTRY) {
    console.error(
      "env.REGISTRY is not setup. Please setup an R2 bucket and add the binding in your wrangler config file. Try 'npx wrangler --env production r2 bucket create r2-registry'",
    );
    return false;
  }

  return true;
};
