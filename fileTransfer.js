/**
 * fileTransfer.js
 *
 * CLI tool to export images from one Shopify store and import them to another.
 *
 * Usage:
 *   node fileTransfer.js export              → fetch all images from source → export.json
 *   node fileTransfer.js import              → upload images from export.json → dest store
 *   node fileTransfer.js transfer            → export then import in one step
 *   node fileTransfer.js refresh-tokens      → force re-fetch both tokens
 *   node fileTransfer.js clear-tokens        → wipe tokens.cache.json
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import * as dotenv from "dotenv";
import { getToken, refreshToken, clearTokenCache } from "./auth.js";

dotenv.config();

const __dirname  = path.dirname(fileURLToPath(import.meta.url));
const EXPORT_FILE = path.join(__dirname, "export.json");
const API_VERSION = "2024-10";

// ─── Logging ─────────────────────────────────────────────────────────────────
const log     = (msg) => console.log(`  ${msg}`);
const ok      = (msg) => console.log(`✅ ${msg}`);
const warn    = (msg) => console.warn(`⚠️  ${msg}`);
const fail    = (msg) => { console.error(`❌ ${msg}`); process.exit(1); };
const divider = (msg) => console.log(`\n${"─".repeat(55)}\n  ${msg}\n${"─".repeat(55)}`);
const sleep   = (ms)  => new Promise((r) => setTimeout(r, ms));

// ─── GraphQL client ───────────────────────────────────────────────────────────
async function gql(store, token, query, variables = {}) {
  const res = await fetch(
    `https://${store}/admin/api/${API_VERSION}/graphql.json`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
      },
      body: JSON.stringify({ query, variables }),
    }
  );

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status} from ${store}: ${text}`);
  }

  const json = await res.json();
  if (json.errors?.length) {
    throw new Error(`GraphQL error: ${JSON.stringify(json.errors, null, 2)}`);
  }

  return json.data;
}

// ─── EXPORT ───────────────────────────────────────────────────────────────────
const EXPORT_QUERY = `
  query GetImages($cursor: String) {
    files(first: 50, after: $cursor, query: "media_type:IMAGE") {
      edges {
        cursor
        node {
          ... on MediaImage {
            id
            alt
            createdAt
            mimeType
            image {
              url
              originalSrc
              width
              height
            }
          }
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
`;

async function runExport() {
  divider("EXPORT — Fetching images from source store");

  const store = process.env.FROM_STORE_URL;
  const token = await getToken("from");

  log(`Store: ${store}`);

  const allFiles = [];
  let cursor = null;
  let page   = 1;

  while (true) {
    log(`Fetching page ${page}...`);

    const data  = await gql(store, token, EXPORT_QUERY, { cursor });
    const edges = data.files.edges;

    for (const edge of edges) {
      const node = edge.node;
      // Skip anything that didn't resolve as a MediaImage
      if (!node?.image) continue;

      allFiles.push({
        id:        node.id,
        alt:       node.alt ?? "",
        mimeType:  node.mimeType ?? "image/jpeg",
        url:       node.image.originalSrc ?? node.image.url,
        width:     node.image.width,
        height:    node.image.height,
        createdAt: node.createdAt,
      });
    }

    if (!data.files.pageInfo.hasNextPage) break;
    cursor = data.files.pageInfo.endCursor;
    page++;

    await sleep(300);
  }

  fs.writeFileSync(EXPORT_FILE, JSON.stringify(allFiles, null, 2));
  ok(`Exported ${allFiles.length} images → export.json`);

  return allFiles;
}

// ─── IMPORT ───────────────────────────────────────────────────────────────────
const STAGED_UPLOAD_MUTATION = `
  mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
    stagedUploadsCreate(input: $input) {
      stagedTargets {
        url
        resourceUrl
        parameters { name value }
      }
      userErrors { field message }
    }
  }
`;

const FILE_CREATE_MUTATION = `
  mutation fileCreate($files: [FileCreateInput!]!) {
    fileCreate(files: $files) {
      files {
        ... on MediaImage {
          id
          image { url }
        }
      }
      userErrors { field message }
    }
  }
`;

async function uploadSingleFile(store, token, file) {
  const filename = file.url.split("/").pop().split("?")[0] || "image.jpg";
  const mimeType = file.mimeType ?? "image/jpeg";

  // Step 1 — request a staged upload slot from Shopify
  const stagedData = await gql(store, token, STAGED_UPLOAD_MUTATION, {
    input: [{
      filename,
      mimeType,
      httpMethod: "POST",
      resource: "IMAGE",
    }],
  });

  const { stagedTargets, userErrors } = stagedData.stagedUploadsCreate;
  if (userErrors.length) {
    throw new Error(`Staged upload error: ${JSON.stringify(userErrors)}`);
  }

  const target = stagedTargets[0];

  // Step 2 — fetch the raw image bytes from the source URL
  const imageRes = await fetch(file.url);
  if (!imageRes.ok) {
    throw new Error(`Failed to fetch source image (HTTP ${imageRes.status}): ${file.url}`);
  }
  const imageBuffer = await imageRes.arrayBuffer();

  // Step 3 — POST the image bytes to the staged upload target
  const formData = new FormData();
  for (const param of target.parameters) {
    formData.append(param.name, param.value);
  }
  formData.append("file", new Blob([imageBuffer], { type: mimeType }), filename);

  const uploadRes = await fetch(target.url, { method: "POST", body: formData });
  if (!uploadRes.ok) {
    const text = await uploadRes.text();
    throw new Error(`Staged upload POST failed (HTTP ${uploadRes.status}): ${text}`);
  }

  // Step 4 — register the file in Shopify using the resourceUrl
  const createData = await gql(store, token, FILE_CREATE_MUTATION, {
    files: [{
      originalSource: target.resourceUrl,
      alt:            file.alt ?? "",
      contentType:    "IMAGE",
    }],
  });

  const createErrors = createData.fileCreate.userErrors;
  if (createErrors.length) {
    throw new Error(`fileCreate error: ${JSON.stringify(createErrors)}`);
  }

  return createData.fileCreate.files[0];
}

async function runImport(files = null) {
  divider("IMPORT — Uploading images to destination store");

  const store = process.env.TO_STORE_URL;
  const token = await getToken("to");

  // Load from export.json if not passed in directly (i.e. standalone import)
  if (!files) {
    if (!fs.existsSync(EXPORT_FILE)) {
      fail("export.json not found. Run `node fileTransfer.js export` first.");
    }
    files = JSON.parse(fs.readFileSync(EXPORT_FILE, "utf-8"));
  }

  log(`Store:          ${store}`);
  log(`Files to import: ${files.length}`);
  console.log("");

  let successCount = 0;
  let failCount    = 0;

  for (let i = 0; i < files.length; i++) {
    const file  = files[i];
    const label = file.url.split("/").pop().split("?")[0];

    try {
      process.stdout.write(`  [${i + 1}/${files.length}] ${label} ... `);
      await uploadSingleFile(store, token, file);
      console.log("✅");
      successCount++;
    } catch (err) {
      console.log("❌");
      warn(`Failed: ${err.message}`);
      failCount++;
    }

    // Respect Shopify's rate limits
    await sleep(500);
  }

  console.log("");
  ok(`Import complete — ${successCount} succeeded, ${failCount} failed`);
}

// ─── TRANSFER (export + import in one go) ─────────────────────────────────────
async function runTransfer() {
  const files = await runExport();
  await runImport(files);
}

// ─── CLI entry ────────────────────────────────────────────────────────────────
const command = process.argv[2];

switch (command) {
  case "export":
    runExport().catch((e) => fail(e.message));
    break;

  case "import":
    runImport().catch((e) => fail(e.message));
    break;

  case "transfer":
    runTransfer().catch((e) => fail(e.message));
    break;

  case "refresh-tokens":
    refreshToken("both")
      .then(() => ok("Both tokens refreshed."))
      .catch((e) => fail(e.message));
    break;

  case "clear-tokens":
    clearTokenCache();
    break;

  default:
    console.log(`
Shopify File Transfer — Image Migrator

Usage:
  node fileTransfer.js export           Fetch all images from source → export.json
  node fileTransfer.js import           Upload images from export.json → dest store
  node fileTransfer.js transfer         Do both in one step
  node fileTransfer.js refresh-tokens   Force re-fetch both access tokens
  node fileTransfer.js clear-tokens     Wipe the local token cache

Config (.env):
  FROM_STORE_URL        e.g. my-source.myshopify.com
  FROM_CLIENT_ID        Client ID from Dev Dashboard (source app)
  FROM_CLIENT_SECRET    Client Secret from Dev Dashboard (source app)

  TO_STORE_URL          e.g. my-dest.myshopify.com
  TO_CLIENT_ID          Client ID from Dev Dashboard (dest app)
  TO_CLIENT_SECRET      Client Secret from Dev Dashboard (dest app)
`);
}