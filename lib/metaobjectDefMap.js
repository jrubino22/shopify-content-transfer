/**
 * lib/metaobjectDefMap.js
 *
 * Builds a map of metaobject definition GIDs between source and destination stores.
 * Used by metafieldTransfer.js to resolve metaobject definition GID references
 * in metafield definition validations.
 */

import * as dotenv from "dotenv";
dotenv.config();

const API_VERSION = "2024-10";

const FROM_STORE = process.env.FROM_STORE_URL;
const FROM_TOKEN = process.env.FROM_ACCESS_TOKEN;
const TO_STORE   = process.env.TO_STORE_URL;
const TO_TOKEN   = process.env.TO_ACCESS_TOKEN;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const ALL_DEFINITIONS_QUERY = `
  query GetMetaobjectDefinitions($cursor: String) {
    metaobjectDefinitions(first: 50, after: $cursor) {
      edges {
        node {
          id
          type
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

async function fetchAllDefinitions(store, token) {
  const definitions = [];
  let cursor = null;

  while (true) {
    const res = await fetch(
      `https://${store}/admin/api/${API_VERSION}/graphql.json`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Shopify-Access-Token": token,
        },
        body: JSON.stringify({ query: ALL_DEFINITIONS_QUERY, variables: { cursor } }),
      }
    );

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`HTTP ${res.status} from ${store}: ${text}`);
    }

    const json = await res.json();
    if (json.errors?.length) throw new Error(`GraphQL error: ${JSON.stringify(json.errors)}`);

    const edges = json.data.metaobjectDefinitions.edges;
    definitions.push(...edges.map((e) => e.node));

    if (!json.data.metaobjectDefinitions.pageInfo.hasNextPage) break;
    cursor = json.data.metaobjectDefinitions.pageInfo.endCursor;
    await sleep(300);
  }

  return definitions;
}

/**
 * Build a map of source definition GID → dest definition GID,
 * matched by type (which is consistent across stores).
 *
 * e.g. { "gid://shopify/MetaobjectDefinition/123": "gid://shopify/MetaobjectDefinition/456" }
 */
export async function buildMetaobjectDefMap(log) {
  log("Building metaobject definition GID map (source → dest)...");

  const [sourceDefs, destDefs] = await Promise.all([
    fetchAllDefinitions(FROM_STORE, FROM_TOKEN),
    fetchAllDefinitions(TO_STORE, TO_TOKEN),
  ]);

  // Build a type → destGID lookup from the destination store
  const destByType = Object.fromEntries(destDefs.map((d) => [d.type, d.id]));

  const map = {};
  for (const sourceDef of sourceDefs) {
    const destGid = destByType[sourceDef.type];
    if (destGid) map[sourceDef.id] = destGid;
  }

  log(`  → ${Object.keys(map).length} definition GIDs mapped`);
  return map;
}