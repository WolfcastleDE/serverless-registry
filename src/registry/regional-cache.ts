import { Env } from "../..";
import { isValidDigest } from "../user";
import { errorString } from "../utils";

// Value of the `x-registry-cache` response header.
//   hit     -> served from the regional cache bucket
//   miss    -> served from the primary bucket, the regional cache is being filled
//   primary -> served from the primary bucket, no caching applies (tag lookups, no regional bucket, pushes)
export type CacheStatus = "hit" | "miss" | "primary";

export const cacheStatusHeader = "x-registry-cache";

// Objects up to this size are filled by tee()-ing the stream that is already being
// returned to the client. Bigger objects are copied with a separate read from the primary
// bucket instead, because tee() buffers whatever the slower branch has not consumed yet and
// a slow client could otherwise push the Worker over its memory limit.
const defaultTeeMaxBytes = 64 * 1024 * 1024;

// Continent codes as delivered in request.cf.continent.
const continentToCache: Record<string, keyof RegionalCacheBindings> = {
  EU: "REGISTRY_CACHE_EU",
  AF: "REGISTRY_CACHE_EU",
  NA: "REGISTRY_CACHE_US",
  SA: "REGISTRY_CACHE_US",
};

type RegionalCacheBindings = {
  REGISTRY_CACHE_EU?: R2Bucket;
  REGISTRY_CACHE_US?: R2Bucket;
};

// Metadata stored next to every cached object, so a hit does not need the primary bucket.
type CachedObjectMetadata = {
  digest: string;
  contentType?: string;
};

export type CachedObject = {
  stream: ReadableStream;
  digest: string;
  size: number;
  contentType?: string;
};

export type CachedHead = {
  digest: string;
  size: number;
  contentType?: string;
};

// Every regional cache bucket, independent of the request's location. Used to invalidate.
export function allCacheBuckets(env: Env): R2Bucket[] {
  return [env.REGISTRY_CACHE_EU, env.REGISTRY_CACHE_US].filter((b): b is R2Bucket => b !== undefined);
}

// Deletes keys from every regional cache bucket. Errors are logged, not thrown: a stale
// entry in a cache bucket must never make a delete in the primary bucket fail.
export async function invalidateCaches(env: Env, keys: string | string[]): Promise<void> {
  const list = Array.isArray(keys) ? keys : [keys];
  if (list.length === 0) {
    return;
  }

  await Promise.all(
    allCacheBuckets(env).map(async (bucket) => {
      try {
        // R2 accepts at most 1000 keys per delete call
        for (let i = 0; i < list.length; i += 1000) {
          await bucket.delete(list.slice(i, i + 1000));
        }
      } catch (err) {
        console.error("Invalidating regional cache failed:", errorString(err));
      }
    }),
  );
}

// Only content addressed objects are cacheable: blobs and manifests requested by digest.
// A tag can move to another digest at any time, so tag lookups always go to the primary bucket.
export function isCacheableKey(key: string): boolean {
  const blob = key.lastIndexOf("/blobs/");
  const manifest = key.lastIndexOf("/manifests/");
  const index = Math.max(blob, manifest);
  if (index === -1) {
    return false;
  }

  const reference = key.substring(key.indexOf("/", index + 1) + 1);
  return isValidDigest(reference);
}

export class RegionalCache {
  readonly bucket: R2Bucket | undefined;
  private readonly teeMaxBytes: number;

  constructor(
    env: Env,
    continent: string | undefined,
    private readonly context: ExecutionContext | undefined,
  ) {
    const binding = continent ? continentToCache[continent] : undefined;
    this.bucket = binding ? env[binding] : undefined;

    const teeMaxBytes = parseInt(env.CACHE_TEE_MAX_BYTES ?? "", 10);
    this.teeMaxBytes = Number.isFinite(teeMaxBytes) && teeMaxBytes >= 0 ? teeMaxBytes : defaultTeeMaxBytes;
  }

  static fromRequest(env: Env, request: Request, context: ExecutionContext | undefined): RegionalCache {
    const continent = (request.cf as IncomingRequestCfProperties | undefined)?.continent;
    return new RegionalCache(env, continent, context);
  }

  // Whether a lookup for this key can be served from (and fill) the regional cache.
  applies(key: string): boolean {
    return this.bucket !== undefined && this.context !== undefined && isCacheableKey(key);
  }

  async get(key: string): Promise<CachedObject | null> {
    if (!this.applies(key)) {
      return null;
    }

    try {
      const obj = await this.bucket!.get(key);
      if (obj === null) {
        return null;
      }

      const metadata = obj.customMetadata as CachedObjectMetadata | undefined;
      if (!metadata?.digest) {
        // Not written by us, ignore it and let the caller fall back to the primary bucket.
        await obj.body.cancel();
        return null;
      }

      return {
        stream: obj.body,
        digest: metadata.digest,
        size: obj.size,
        contentType: metadata.contentType,
      };
    } catch (err) {
      // A broken cache bucket must never break pulls
      console.error("Reading regional cache failed:", errorString(err));
      return null;
    }
  }

  async head(key: string): Promise<CachedHead | null> {
    if (!this.applies(key)) {
      return null;
    }

    try {
      const obj = await this.bucket!.head(key);
      if (obj === null || !obj.customMetadata?.digest) {
        return null;
      }

      return {
        digest: obj.customMetadata.digest,
        size: obj.size,
        contentType: obj.customMetadata.contentType,
      };
    } catch (err) {
      console.error("Reading regional cache failed:", errorString(err));
      return null;
    }
  }

  // Fills the regional cache with an object that was just read from the primary bucket.
  //
  // Returns the stream that should be sent to the client. For small objects that is one branch
  // of a tee() of `stream`, for big objects it is `stream` itself and the cache is filled by
  // `refetch`, a second independent read from the primary bucket.
  fill(
    key: string,
    stream: ReadableStream,
    object: { digest: string; size: number; contentType?: string },
    refetch: () => Promise<ReadableStream | null>,
  ): ReadableStream {
    if (!this.applies(key)) {
      return stream;
    }

    const bucket = this.bucket!;
    const metadata: CachedObjectMetadata = { digest: object.digest };
    if (object.contentType) {
      metadata.contentType = object.contentType;
    }

    const write = async (source: ReadableStream) => {
      // R2 needs to know the length of a streamed body upfront
      const fixed = new FixedLengthStream(object.size);
      const [, err] = await Promise.all([
        source.pipeTo(fixed.writable).catch((e: unknown) => e),
        bucket
          .put(key, fixed.readable, {
            ...(object.digest.startsWith("sha256:") ? { sha256: object.digest.substring("sha256:".length) } : {}),
            httpMetadata: object.contentType ? { contentType: object.contentType } : undefined,
            customMetadata: metadata,
          })
          .then(() => undefined)
          .catch((e: unknown) => e),
      ]);
      if (err !== undefined) {
        console.error(`Filling regional cache for ${key} failed:`, errorString(err));
      }
    };

    if (object.size <= this.teeMaxBytes) {
      const [toClient, toCache] = stream.tee();
      this.context!.waitUntil(write(toCache));
      return toClient;
    }

    this.context!.waitUntil(
      (async () => {
        try {
          const source = await refetch();
          if (source !== null) {
            await write(source);
          }
        } catch (err) {
          console.error(`Filling regional cache for ${key} failed:`, errorString(err));
        }
      })(),
    );
    return stream;
  }
}
