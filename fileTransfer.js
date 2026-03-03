/**
 * fileTransfer.js
 *
 * Transfers image files from one Shopify store to another.
 * Skips images that already exist in the destination store (matched by filename).
 * Requires permanent access tokens in .env (use shopify-token-generator to get them).
 *
 * Usage:
 *   node fileTransfer.js export      Fetch all images from source store → export.json
 *   node fileTransfer.js import      Upload images from export.json → dest store
 *   node fileTransfer.js transfer    Do both in one step
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import * as dotenv from "dotenv";
import { buildDestImageMap, extractFilename } from "./lib/imageMap.js";

dotenv.config();

const __dirname   = path.dirname(fileURLToPath(import.meta.url));
const EXPORT_FILE = path.join(__dirname, "export.json");
const API_VERSION = "2024-10";

const FROM_STORE = process.env.FROM_STORE_URL;
const FROM_TOKEN = process.env.FROM_ACCESS_TOKEN;
const TO_STORE   = process.env.TO_STORE_URL;
const TO_TOKEN   = process.env.TO_ACCESS_TOKEN;

// ─── Logging ──────────────────────────────────────────────────────────────────
const log     = (msg) => console.log(`  ${msg}`);
const ok      = (msg) => console.log(`✅ ${msg}`);
const warn    = (msg) => console.warn(`⚠️  ${msg}`);
const fail    = (msg) => { console.error(`❌ ${msg}`); process.exit(1); };
const divider = (msg) => console.log(`\n${"─".repeat(55)}\n  ${msg}\n${"─".repeat(55)}`);
const sleep   = (ms)  => new Promise((r) => setTimeout(r, ms));

// ─── Validate env ─────────────────────────────────────────────────────────────
function validateEnv(mode) {
  const missing = [];
  if (mode === "export" || mode === "transfer") {
    if (!FROM_STORE) missing.push("FROM_STORE_URL");
    if (!FROM_TOKEN) missing.push("FROM_ACCESS_TOKEN");
  }
  if (mode === "import" || mode === "transfer") {
    if (!TO_STORE) missing.push("TO_STORE_URL");
    if (!TO_TOKEN) missing.push("TO_ACCESS_TOKEN");
  }
  if (missing.length) {
    fail(`Missing required .env values:\n\n  ${missing.join("\n  ")}\n\n  See .env.example — use shopify-token-generator to get tokens.`);
  }
}

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
  validateEnv("export");
  log(`Store: ${FROM_STORE}`);

  const allFiles = [];
  let cursor = null;
  let page   = 1;

  while (true) {
    log(`Fetching page ${page}...`);

    const data  = await gql(FROM_STORE, FROM_TOKEN, EXPORT_QUERY, { cursor });
    const edges = data.files.edges;

    for (const edge of edges) {
      const node = edge.node;
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

async function uploadSingleFile(file) {
  const filename = extractFilename(file.url);
  const mimeType = file.mimeType ?? "image/jpeg";

  // Step 1 — request a staged upload target
  const stagedData = await gql(TO_STORE, TO_TOKEN, STAGED_UPLOAD_MUTATION, {
    input: [{
      filename,
      mimeType,
      httpMethod: "POST",
      resource:   "IMAGE",
    }],
  });

  const { stagedTargets, userErrors } = stagedData.stagedUploadsCreate;
  if (userErrors.length) {
    throw new Error(`Staged upload error: ${JSON.stringify(userErrors)}`);
  }

  const target = stagedTargets[0];

  // Step 2 — fetch raw image bytes from source URL
  const imageRes = await fetch(file.url);
  if (!imageRes.ok) {
    throw new Error(`Failed to fetch source image (HTTP ${imageRes.status}): ${file.url}`);
  }
  const imageBuffer = await imageRes.arrayBuffer();

  // Step 3 — POST image to staged upload target
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

  // Step 4 — register the file in Shopify
  const createData = await gql(TO_STORE, TO_TOKEN, FILE_CREATE_MUTATION, {
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
  validateEnv("import");

  if (!files) {
    if (!fs.existsSync(EXPORT_FILE)) {
      fail("export.json not found. Run `node fileTransfer.js export` first.");
    }
    files = JSON.parse(fs.readFileSync(EXPORT_FILE, "utf-8"));
  }

  log(`Store:           ${TO_STORE}`);
  log(`Files to import: ${files.length}`);

  // Build destination image map upfront so we can skip existing files
  divider("Checking existing images in destination store");
  const destImageMap = await buildDestImageMap(log);

  console.log("");

  let successCount = 0;
  let skippedCount = 0;
  let failCount    = 0;

  for (let i = 0; i < files.length; i++) {
    const file     = files[i];
    const filename = extractFilename(file.url);

    // Skip if filename already exists in destination store
    if (destImageMap[filename]) {
      process.stdout.write(`  [${i + 1}/${files.length}] ${filename} ... `);
      console.log("⏭  skipped");
      skippedCount++;
      continue;
    }

    try {
      process.stdout.write(`  [${i + 1}/${files.length}] ${filename} ... `);
      await uploadSingleFile(file);
      console.log("✅");
      successCount++;
    } catch (err) {
      console.log("❌");
      warn(`Failed: ${err.message}`);
      failCount++;
    }

    await sleep(500);
  }

  console.log(`
${"─".repeat(55)}
  SUMMARY
${"─".repeat(55)}
  Uploaded: ${successCount}
  Skipped:  ${skippedCount}
  Failed:   ${failCount}
${"─".repeat(55)}
`);
}

// ─── Transfer (export + import) ───────────────────────────────────────────────
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

  default:
    console.log(`
Shopify File Transfer — Image Migrator

Usage:
  node fileTransfer.js export      Fetch all images from source store → export.json
  node fileTransfer.js import      Upload images from export.json → dest store
  node fileTransfer.js transfer    Do both in one step

Config (.env):
  FROM_STORE_URL       Source store domain  e.g. my-source.myshopify.com
  FROM_ACCESS_TOKEN    Source store token   (use shopify-token-generator)
  TO_STORE_URL         Dest store domain    e.g. my-dest.myshopify.com
  TO_ACCESS_TOKEN      Dest store token     (use shopify-token-generator)
`);
}