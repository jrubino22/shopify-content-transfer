/**
 * lib/imageMap.js
 *
 * Builds image ID reference maps between source and destination stores.
 * Used by metaobjectTransfer.js and metafieldTransfer.js to resolve
 * MediaImage GID references across stores.
 */

import * as dotenv from "dotenv";
dotenv.config();

const API_VERSION = "2024-10";

const FROM_STORE = process.env.FROM_STORE_URL;
const FROM_TOKEN = process.env.FROM_ACCESS_TOKEN;
const TO_STORE   = process.env.TO_STORE_URL;
const TO_TOKEN   = process.env.TO_ACCESS_TOKEN;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const ALL_IMAGES_QUERY = `
  query GetAllImages($cursor: String) {
    files(first: 50, after: $cursor, query: "media_type:IMAGE") {
      edges {
        cursor
        node {
          ... on MediaImage {
            id
            image { url }
          }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

async function fetchAllImages(store, token) {
  const images = [];
  let cursor   = null;

  while (true) {
    const res = await fetch(
      `https://${store}/admin/api/${API_VERSION}/graphql.json`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Shopify-Access-Token": token,
        },
        body: JSON.stringify({ query: ALL_IMAGES_QUERY, variables: { cursor } }),
      }
    );

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`HTTP ${res.status} from ${store}: ${text}`);
    }

    const json = await res.json();
    if (json.errors?.length) throw new Error(`GraphQL error: ${JSON.stringify(json.errors)}`);

    const edges = json.data.files.edges;
    images.push(...edges.map((e) => e.node).filter((n) => n?.image?.url));

    if (!json.data.files.pageInfo.hasNextPage) break;
    cursor = json.data.files.pageInfo.endCursor;
    await sleep(300);
  }

  return images;
}

/**
 * Extract filename from a Shopify CDN URL.
 * e.g. https://cdn.shopify.com/s/files/1/0623/files/my-image.jpg?v=123 → my-image.jpg
 */
export function extractFilename(url) {
  if (!url) return null;
  try {
    return url.split("?")[0].split("/").pop();
  } catch {
    return null;
  }
}

/**
 * Build a map of sourceGID → filename from the source store.
 */
export async function buildSourceImageMap(log) {
  log("Building source image map (GID → filename)...");
  const images = await fetchAllImages(FROM_STORE, FROM_TOKEN);
  const map    = {};

  for (const node of images) {
    const filename = extractFilename(node.image.url);
    if (filename) map[node.id] = filename;
  }

  log(`  → ${Object.keys(map).length} source images mapped`);
  return map;
}

/**
 * Build a map of filename → destGID from the destination store.
 */
export async function buildDestImageMap(log) {
  log("Building destination image map (filename → GID)...");
  const images = await fetchAllImages(TO_STORE, TO_TOKEN);
  const map    = {};

  for (const node of images) {
    const filename = extractFilename(node.image.url);
    if (filename) map[filename] = node.id;
  }

  log(`  → ${Object.keys(map).length} destination images mapped`);
  return map;
}

/**
 * Build both maps in one call.
 * Returns { sourceImageMap, destImageMap }
 */
export async function buildImageMaps(log) {
  const sourceImageMap = await buildSourceImageMap(log);
  const destImageMap   = await buildDestImageMap(log);
  return { sourceImageMap, destImageMap };
}