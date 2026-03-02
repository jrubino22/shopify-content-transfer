/**
 * auth.js
 *
 * Handles Shopify access token acquisition and caching.
 * Tokens are valid for 24 hours — this module caches them locally
 * in tokens.cache.json and only re-fetches when expired.
 *
 * Usage:
 *   import { getToken } from "./auth.js";
 *   const token = await getToken("from"); // or "to"
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import * as dotenv from "dotenv";

dotenv.config();

const __dirname  = path.dirname(fileURLToPath(import.meta.url));
const CACHE_FILE = path.join(__dirname, "tokens.cache.json");

// Shave 5 minutes off expiry to avoid using a token right as it expires
const EXPIRY_BUFFER_MS = 5 * 60 * 1000;

// ─── Config map ──────────────────────────────────────────────────────────────
const STORE_CONFIG = {
  from: {
    store:        () => process.env.FROM_STORE_URL,
    clientId:     () => process.env.FROM_CLIENT_ID,
    clientSecret: () => process.env.FROM_CLIENT_SECRET,
  },
  to: {
    store:        () => process.env.TO_STORE_URL,
    clientId:     () => process.env.TO_CLIENT_ID,
    clientSecret: () => process.env.TO_CLIENT_SECRET,
  },
};

// ─── Cache helpers ───────────────────────────────────────────────────────────
function readCache() {
  if (!fs.existsSync(CACHE_FILE)) return {};
  try {
    return JSON.parse(fs.readFileSync(CACHE_FILE, "utf-8"));
  } catch {
    return {};
  }
}

function writeCache(cache) {
  fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2));
}

function getCachedToken(key) {
  const cache = readCache();
  const entry = cache[key];
  if (!entry) return null;

  const isExpired = Date.now() >= entry.expiresAt - EXPIRY_BUFFER_MS;
  if (isExpired) {
    console.log(`  🔄 Cached token for "${key}" store is expired, refreshing...`);
    return null;
  }

  return entry.token;
}

function setCachedToken(key, token, expiresInSeconds) {
  const cache = readCache();
  cache[key] = {
    token,
    expiresAt: Date.now() + expiresInSeconds * 1000,
    cachedAt:  new Date().toISOString(),
  };
  writeCache(cache);
}

// ─── Token fetch ─────────────────────────────────────────────────────────────
async function fetchFreshToken(key) {
  const config       = STORE_CONFIG[key];
  const store        = config.store();
  const clientId     = config.clientId();
  const clientSecret = config.clientSecret();

  if (!store || !clientId || !clientSecret) {
    const p = key.toUpperCase();
    throw new Error(
      `Missing env vars for "${key}" store. Required:\n` +
      `  ${p}_STORE_URL\n  ${p}_CLIENT_ID\n  ${p}_CLIENT_SECRET`
    );
  }

  console.log(`  🔑 Fetching fresh token for "${key}" store (${store})...`);

  const res = await fetch(`https://${store}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type:    "client_credentials",
      client_id:     clientId,
      client_secret: clientSecret,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Token request failed for "${key}" store (HTTP ${res.status}): ${text}`);
  }

  const json = await res.json();

  if (!json.access_token) {
    throw new Error(`No access_token in response: ${JSON.stringify(json)}`);
  }

  // expires_in is in seconds — Shopify returns 86399 (~24h)
  setCachedToken(key, json.access_token, json.expires_in ?? 86399);
  console.log(`  ✅ Token acquired (valid for ~${Math.round((json.expires_in ?? 86399) / 3600)}h)`);

  return json.access_token;
}

// ─── Public API ──────────────────────────────────────────────────────────────

/**
 * Get a valid access token for the given store key ("from" or "to").
 * Returns a cached token if still valid, otherwise fetches a fresh one.
 *
 * @param {"from"|"to"} key
 * @returns {Promise<string>} access token
 */
export async function getToken(key) {
  if (!STORE_CONFIG[key]) {
    throw new Error(`Unknown store key "${key}". Must be "from" or "to".`);
  }
  const cached = getCachedToken(key);
  if (cached) {
    console.log(`  ✅ Using cached token for "${key}" store`);
    return cached;
  }
  return fetchFreshToken(key);
}

/**
 * Force-refresh tokens for one or both stores, ignoring the cache.
 * Useful if a token has been revoked or you want to reset.
 *
 * @param {"from"|"to"|"both"} key
 */
export async function refreshToken(key) {
  const keys = key === "both" ? ["from", "to"] : [key];
  for (const k of keys) {
    await fetchFreshToken(k);
  }
}

/**
 * Clear the local token cache entirely.
 */
export function clearTokenCache() {
  if (fs.existsSync(CACHE_FILE)) {
    fs.unlinkSync(CACHE_FILE);
    console.log("  🗑️  Token cache cleared.");
  }
}

// ─── CLI entry (run directly to fetch/refresh tokens) ────────────────────────
// Usage: node auth.js
//        node auth.js refresh
//        node auth.js clear

const isMain = process.argv[1]?.endsWith("auth.js");

if (isMain) {
  const command = process.argv[2];

  switch (command) {
    case "refresh":
      console.log("\nForce-refreshing tokens for both stores...\n");
      refreshToken("both")
        .then(() => console.log("\n✅ Both tokens refreshed and cached."))
        .catch((e) => { console.error(`❌ ${e.message}`); process.exit(1); });
      break;

    case "clear":
      clearTokenCache();
      break;

    default:
      console.log("\nFetching tokens for both stores...\n");
      Promise.all([getToken("from"), getToken("to")])
        .then(() => console.log("\n✅ Both tokens ready and cached."))
        .catch((e) => { console.error(`❌ ${e.message}`); process.exit(1); });
  }
}