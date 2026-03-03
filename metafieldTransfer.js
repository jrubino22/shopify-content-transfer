/**
 * metafieldTransfer.js
 *
 * Transfers product and variant metafield definitions and values
 * from one Shopify store to another.
 * Skips definitions/values that already exist in the destination store.
 * Resolves image GID references by filename using lib/imageMap.js.
 * Resolves metaobject definition GID references in validations using lib/metaobjectDefMap.js.
 * Resolves metaobject entry GID references in values using metaobjects_export.json.
 *
 * Usage:
 *   node metafieldTransfer.js export
 *   node metafieldTransfer.js import
 *   node metafieldTransfer.js transfer
 *
 * Note: Run metaobjectTransfer.js export before running this script
 * so metaobjects_export.json is available for resolving entry GID references.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import * as dotenv from "dotenv";
import { buildImageMaps } from "./lib/imageMap.js";
import { buildMetaobjectDefMap } from "./lib/metaobjectDefMap.js";

dotenv.config();

const __dirname            = path.dirname(fileURLToPath(import.meta.url));
const EXPORT_FILE          = path.join(__dirname, "metafields_export.json");
const METAOBJECTS_EXPORT   = path.join(__dirname, "metaobjects_export.json");
const API_VERSION          = "2024-10";

const FROM_STORE  = process.env.FROM_STORE_URL;
const FROM_TOKEN  = process.env.FROM_ACCESS_TOKEN;
const TO_STORE    = process.env.TO_STORE_URL;
const TO_TOKEN    = process.env.TO_ACCESS_TOKEN;

const OWNER_TYPES = ["PRODUCT", "PRODUCTVARIANT"];

// ─── Logging ──────────────────────────────────────────────────────────────────
const log     = (msg) => console.log(`  ${msg}`);
const ok      = (msg) => console.log(`✅ ${msg}`);
const warn    = (msg) => console.warn(`⚠️  ${msg}`);
const fail    = (msg) => { console.error(`❌ ${msg}`); process.exit(1); };
const divider = (msg) => console.log(`\n${"─".repeat(55)}\n  ${msg}\n${"─".repeat(55)}`);
const sleep   = (ms)  => new Promise((r) => setTimeout(r, ms));

// ─── Summary ──────────────────────────────────────────────────────────────────
const summary = {
  definitions: { created: 0, skipped: 0, failed: 0, warnings: [] },
  values:      { created: 0, skipped: 0, failed: 0, warnings: [] },
};

function printSummary() {
  console.log(`
${"─".repeat(55)}
  SUMMARY
${"─".repeat(55)}
  Definitions
    Created:  ${summary.definitions.created}
    Skipped:  ${summary.definitions.skipped}
    Failed:   ${summary.definitions.failed}

  Values
    Created:  ${summary.values.created}
    Skipped:  ${summary.values.skipped}
    Failed:   ${summary.values.failed}
`);

  const allWarnings = [
    ...summary.definitions.warnings,
    ...summary.values.warnings,
  ];

  if (allWarnings.length) {
    console.log(`  Warnings (${allWarnings.length}):`);
    allWarnings.forEach((w) => console.log(`  ⚠️  ${w}`));
    console.log("");
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

// ─── GID helpers ──────────────────────────────────────────────────────────────
function isGid(value) {
  return typeof value === "string" && value.startsWith("gid://shopify/");
}

function getGidType(gid) {
  if (!gid || typeof gid !== "string") return null;
  const match = gid.match(/^gid:\/\/shopify\/(\w+)\//);
  return match ? match[1] : null;
}

// ─── Metaobject entry lookup (from export file) ───────────────────────────────

// Build a map of sourceGID → { handle, type } from metaobjects_export.json
function buildSourceMetaobjectEntryMap(metaobjectsExport) {
  const map = {};
  for (const { definition, entries } of metaobjectsExport) {
    for (const entry of entries) {
      map[entry.id] = { handle: entry.handle, type: definition.type };
    }
  }
  return map;
}

// Cache of "type:handle" → destGID to avoid redundant queries
const metaobjectEntryCache = {};

const GET_METAOBJECT_BY_HANDLE_QUERY = `
  query GetMetaobjectByHandle($type: String!, $handle: String!) {
    metaobjectByHandle(handle: { type: $type, handle: $handle }) {
      id
    }
  }
`;

async function resolveMetaobjectEntryGid(type, handle) {
  const cacheKey = `${type}:${handle}`;
  if (metaobjectEntryCache[cacheKey]) return metaobjectEntryCache[cacheKey];

  const data = await gql(TO_STORE, TO_TOKEN, GET_METAOBJECT_BY_HANDLE_QUERY, { type, handle });
  const gid  = data.metaobjectByHandle?.id ?? null;

  if (gid) metaobjectEntryCache[cacheKey] = gid;
  return gid;
}

// ─── Validation GID resolver ──────────────────────────────────────────────────
function resolveValidations(validations, metaobjectDefMap) {
  if (!validations?.length) return validations;

  return validations.map((v) => {
    if (isGid(v.value) && getGidType(v.value) === "MetaobjectDefinition") {
      const destGid = metaobjectDefMap[v.value];
      if (!destGid) {
        throw new Error(
          `Could not resolve MetaobjectDefinition GID ${v.value} — ` +
          `make sure metaobjectTransfer.js has been run first`
        );
      }
      return { ...v, value: destGid };
    }
    return v;
  });
}

// ─── Field value resolver ─────────────────────────────────────────────────────
async function resolveMetafieldValue(value, sourceImageMap, destImageMap, sourceEntryMap) {
  const warnings = [];

  if (!value) return { resolvedValue: value, warnings };

  // Single GID reference
  if (isGid(value)) {
    const type = getGidType(value);

    if (type === "MediaImage") {
      const filename = sourceImageMap[value];
      if (!filename) {
        warnings.push(`No filename found for source image GID ${value}`);
        return { resolvedValue: value, warnings };
      }
      const destGid = destImageMap[filename];
      if (!destGid) {
        warnings.push(`Image "${filename}" not found in destination store — run fileTransfer.js first`);
        return { resolvedValue: value, warnings };
      }
      return { resolvedValue: destGid, warnings };
    }

    if (type === "Metaobject") {
      const entry = sourceEntryMap[value];
      if (!entry) {
        warnings.push(`Source metaobject GID ${value} not found in metaobjects_export.json`);
        return { resolvedValue: value, warnings };
      }
      const destGid = await resolveMetaobjectEntryGid(entry.type, entry.handle);
      if (!destGid) {
        warnings.push(`Metaobject "${entry.handle}" (${entry.type}) not found in destination store — run metaobjectTransfer.js first`);
        return { resolvedValue: value, warnings };
      }
      return { resolvedValue: destGid, warnings };
    }
  }

  // JSON array of GID references
  if (value.startsWith("[")) {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed) && parsed.length > 0 && parsed.every(isGid)) {
        const resolved = [];
        for (const gid of parsed) {
          const type = getGidType(gid);

          if (type === "MediaImage") {
            const filename = sourceImageMap[gid];
            if (!filename) {
              warnings.push(`No filename found for source image GID ${gid}`);
              resolved.push(gid);
              continue;
            }
            const destGid = destImageMap[filename];
            if (!destGid) {
              warnings.push(`Image "${filename}" not found in destination store — run fileTransfer.js first`);
              resolved.push(gid);
              continue;
            }
            resolved.push(destGid);
            continue;
          }

          if (type === "Metaobject") {
            const entry = sourceEntryMap[gid];
            if (!entry) {
              warnings.push(`Source metaobject GID ${gid} not found in metaobjects_export.json`);
              resolved.push(gid);
              continue;
            }
            const destGid = await resolveMetaobjectEntryGid(entry.type, entry.handle);
            if (!destGid) {
              warnings.push(`Metaobject "${entry.handle}" (${entry.type}) not found in destination store — run metaobjectTransfer.js first`);
              resolved.push(gid);
              continue;
            }
            resolved.push(destGid);
            continue;
          }

          // Unknown GID type — pass through as-is
          resolved.push(gid);
        }
        return { resolvedValue: JSON.stringify(resolved), warnings };
      }
    } catch { /* not a JSON array, fall through */ }
  }

  return { resolvedValue: value, warnings };
}

// ─── EXPORT ───────────────────────────────────────────────────────────────────
const METAFIELD_DEFINITIONS_QUERY = `
  query GetMetafieldDefinitions($ownerType: MetafieldOwnerType!, $cursor: String) {
    metafieldDefinitions(ownerType: $ownerType, first: 50, after: $cursor) {
      edges {
        cursor
        node {
          id
          name
          namespace
          key
          description
          type { name }
          ownerType
          validations { name value }
          access { admin storefront }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

const PRODUCTS_WITH_METAFIELDS_QUERY = `
  query GetProductsWithMetafields($cursor: String) {
    products(first: 50, after: $cursor) {
      edges {
        cursor
        node {
          id
          handle
          metafields(first: 50) {
            edges {
              node { namespace key value type }
            }
          }
          variants(first: 100) {
            edges {
              node {
                id
                title
                metafields(first: 50) {
                  edges {
                    node { namespace key value type }
                  }
                }
              }
            }
          }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

async function fetchDefinitionsForOwnerType(ownerType) {
  const definitions = [];
  let cursor = null;

  while (true) {
    const data  = await gql(FROM_STORE, FROM_TOKEN, METAFIELD_DEFINITIONS_QUERY, { ownerType, cursor });
    const edges = data.metafieldDefinitions.edges;
    definitions.push(...edges.map((e) => e.node));
    if (!data.metafieldDefinitions.pageInfo.hasNextPage) break;
    cursor = data.metafieldDefinitions.pageInfo.endCursor;
    await sleep(300);
  }

  return definitions;
}

async function fetchAllProductsWithMetafields() {
  const products = [];
  let cursor = null;

  while (true) {
    const data  = await gql(FROM_STORE, FROM_TOKEN, PRODUCTS_WITH_METAFIELDS_QUERY, { cursor });
    const edges = data.products.edges;

    for (const edge of edges) {
      const product = edge.node;
      products.push({
        id:         product.id,
        handle:     product.handle,
        metafields: product.metafields.edges.map((e) => e.node).filter((m) => m.value !== null),
        variants:   product.variants.edges.map((ve) => ({
          id:         ve.node.id,
          title:      ve.node.title,
          metafields: ve.node.metafields.edges.map((e) => e.node).filter((m) => m.value !== null),
        })),
      });
    }

    if (!data.products.pageInfo.hasNextPage) break;
    cursor = data.products.pageInfo.endCursor;
    await sleep(300);
  }

  return products;
}

async function runExport() {
  divider("EXPORT — Fetching metafields from source store");

  if (!FROM_STORE || !FROM_TOKEN) fail("Missing FROM_STORE_URL or FROM_ACCESS_TOKEN in .env");

  log(`Store: ${FROM_STORE}`);

  const definitions = {};
  for (const ownerType of OWNER_TYPES) {
    log(`Fetching ${ownerType} definitions...`);
    definitions[ownerType] = await fetchDefinitionsForOwnerType(ownerType);
    log(`  → ${definitions[ownerType].length} definitions`);
  }

  log("Fetching products and variant metafield values...");
  const products = await fetchAllProductsWithMetafields();
  log(`  → ${products.length} products`);

  const result = { definitions, products };
  fs.writeFileSync(EXPORT_FILE, JSON.stringify(result, null, 2));

  const totalDefs   = Object.values(definitions).reduce((a, d) => a + d.length, 0);
  const totalValues = products.reduce((a, p) => {
    return a + p.metafields.length + p.variants.reduce((b, v) => b + v.metafields.length, 0);
  }, 0);

  ok(`Exported ${totalDefs} definitions, ${totalValues} metafield values → metafields_export.json`);

  return result;
}

// ─── IMPORT ───────────────────────────────────────────────────────────────────
const GET_METAFIELD_DEFINITION_QUERY = `
  query GetMetafieldDefinition($ownerType: MetafieldOwnerType!, $namespace: String!) {
    metafieldDefinitions(ownerType: $ownerType, namespace: $namespace, first: 50) {
      edges {
        node {
          id
          namespace
          key
          type { name }
        }
      }
    }
  }
`;

const CREATE_METAFIELD_DEFINITION_MUTATION = `
  mutation CreateMetafieldDefinition($definition: MetafieldDefinitionInput!) {
    metafieldDefinitionCreate(definition: $definition) {
      createdDefinition { id namespace key }
      userErrors { field message code }
    }
  }
`;

const GET_PRODUCT_BY_HANDLE_QUERY = `
  query GetProductByHandle($handle: String!) {
    productByHandle(handle: $handle) {
      id
      handle
      metafields(first: 50) {
        edges {
          node { namespace key value }
        }
      }
      variants(first: 100) {
        edges {
          node {
            id
            title
            metafields(first: 50) {
              edges {
                node { namespace key value }
              }
            }
          }
        }
      }
    }
  }
`;

const SET_METAFIELD_MUTATION = `
  mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) {
      metafields { namespace key value }
      userErrors { field message code }
    }
  }
`;

async function definitionExists(ownerType, namespace, key) {
  const data = await gql(TO_STORE, TO_TOKEN, GET_METAFIELD_DEFINITION_QUERY, { ownerType, namespace });
  return data.metafieldDefinitions.edges.some(
    (e) => e.node.namespace === namespace && e.node.key === key
  );
}

function buildDefinitionInput(def, metaobjectDefMap) {
  const isReservedNamespace = def.namespace.startsWith("$app:") || def.namespace.startsWith("app--");

  const input = {
    name:      def.name,
    namespace: def.namespace,
    key:       def.key,
    type:      def.type.name,
    ownerType: def.ownerType,
  };

  if (def.description) input.description = def.description;

  if (def.validations?.length) {
    input.validations = resolveValidations(def.validations, metaobjectDefMap);
  }

  if (isReservedNamespace && (def.access?.admin || def.access?.storefront)) {
    input.access = {};
    if (def.access.admin)      input.access.admin      = def.access.admin;
    if (def.access.storefront) input.access.storefront = def.access.storefront;
  }

  return input;
}

async function importDefinition(def, metaobjectDefMap) {
  const exists = await definitionExists(def.ownerType, def.namespace, def.key);

  if (exists) {
    log(`  Definition "${def.namespace}.${def.key}" (${def.ownerType}) already exists — skipping`);
    summary.definitions.skipped++;
    return false;
  }

  const data = await gql(TO_STORE, TO_TOKEN, CREATE_METAFIELD_DEFINITION_MUTATION, {
    definition: buildDefinitionInput(def, metaobjectDefMap),
  });

  const { userErrors } = data.metafieldDefinitionCreate;
  if (userErrors.length) throw new Error(userErrors.map((e) => e.message).join(", "));

  summary.definitions.created++;
  return true;
}

function metafieldAlreadyExists(existingMetafields, namespace, key) {
  return existingMetafields.some((m) => m.namespace === namespace && m.key === key);
}

async function setMetafield(ownerId, namespace, key, value, type) {
  const data = await gql(TO_STORE, TO_TOKEN, SET_METAFIELD_MUTATION, {
    metafields: [{ ownerId, namespace, key, value, type }],
  });

  const { userErrors } = data.metafieldsSet;
  if (userErrors.length) throw new Error(userErrors.map((e) => e.message).join(", "));
}

async function importProductMetafields(product, sourceImageMap, destImageMap, sourceEntryMap) {
  if (!product.metafields.length && !product.variants.some((v) => v.metafields.length)) return;

  const data        = await gql(TO_STORE, TO_TOKEN, GET_PRODUCT_BY_HANDLE_QUERY, { handle: product.handle });
  const destProduct = data.productByHandle;

  if (!destProduct) {
    const msg = `Product "${product.handle}" not found in destination store — skipping`;
    warn(msg);
    summary.values.warnings.push(msg);
    const skippedCount = product.metafields.length +
      product.variants.reduce((a, v) => a + v.metafields.length, 0);
    summary.values.skipped += skippedCount;
    return;
  }

  const existingProductMetafields = destProduct.metafields.edges.map((e) => e.node);

  // Product-level metafields
  for (const mf of product.metafields) {
    const label = `${product.handle} → ${mf.namespace}.${mf.key}`;
    process.stdout.write(`    [product] ${label} ... `);

    try {
      if (metafieldAlreadyExists(existingProductMetafields, mf.namespace, mf.key)) {
        console.log("⏭  skipped");
        summary.values.skipped++;
        continue;
      }

      const { resolvedValue, warnings: refWarnings } = await resolveMetafieldValue(
        mf.value, sourceImageMap, destImageMap, sourceEntryMap
      );

      if (refWarnings.length) {
        refWarnings.forEach((w) => summary.values.warnings.push(`Product metafield "${label}": ${w}`));
      }

      await setMetafield(destProduct.id, mf.namespace, mf.key, resolvedValue, mf.type);
      console.log("✅");
      summary.values.created++;
    } catch (err) {
      console.log("❌");
      warn(`Failed: ${err.message}`);
      summary.values.failed++;
      summary.values.warnings.push(`Product metafield "${label}": ${err.message}`);
    }

    await sleep(200);
  }

  // Variant-level metafields
  const destVariantMap = Object.fromEntries(
    destProduct.variants.edges.map((e) => [e.node.title, e.node])
  );

  for (const variant of product.variants) {
    if (!variant.metafields.length) continue;

    const destVariant = destVariantMap[variant.title];
    if (!destVariant) {
      const msg = `Variant "${variant.title}" on product "${product.handle}" not found in destination store — skipping`;
      warn(msg);
      summary.values.warnings.push(msg);
      summary.values.skipped += variant.metafields.length;
      continue;
    }

    const existingVariantMetafields = destVariant.metafields.edges.map((e) => e.node);

    for (const mf of variant.metafields) {
      const label = `${product.handle}/${variant.title} → ${mf.namespace}.${mf.key}`;
      process.stdout.write(`    [variant] ${label} ... `);

      try {
        if (metafieldAlreadyExists(existingVariantMetafields, mf.namespace, mf.key)) {
          console.log("⏭  skipped");
          summary.values.skipped++;
          continue;
        }

        const { resolvedValue, warnings: refWarnings } = await resolveMetafieldValue(
          mf.value, sourceImageMap, destImageMap, sourceEntryMap
        );

        if (refWarnings.length) {
          refWarnings.forEach((w) => summary.values.warnings.push(`Variant metafield "${label}": ${w}`));
        }

        await setMetafield(destVariant.id, mf.namespace, mf.key, resolvedValue, mf.type);
        console.log("✅");
        summary.values.created++;
      } catch (err) {
        console.log("❌");
        warn(`Failed: ${err.message}`);
        summary.values.failed++;
        summary.values.warnings.push(`Variant metafield "${label}": ${err.message}`);
      }

      await sleep(200);
    }
  }
}

async function runImport(exportedData = null) {
  divider("IMPORT — Transferring metafields to destination store");

  if (!TO_STORE || !TO_TOKEN) fail("Missing TO_STORE_URL or TO_ACCESS_TOKEN in .env");

  if (!exportedData) {
    if (!fs.existsSync(EXPORT_FILE)) {
      fail("metafields_export.json not found. Run `node metafieldTransfer.js export` first.");
    }
    exportedData = JSON.parse(fs.readFileSync(EXPORT_FILE, "utf-8"));
  }

  // Load metaobjects export for entry GID resolution
  if (!fs.existsSync(METAOBJECTS_EXPORT)) {
    fail("metaobjects_export.json not found. Run `node metaobjectTransfer.js export` first.");
  }
  const metaobjectsExport = JSON.parse(fs.readFileSync(METAOBJECTS_EXPORT, "utf-8"));
  const sourceEntryMap    = buildSourceMetaobjectEntryMap(metaobjectsExport);
  log(`Loaded ${Object.keys(sourceEntryMap).length} metaobject entries from metaobjects_export.json`);

  const totalDefs = Object.values(exportedData.definitions).reduce((a, d) => a + d.length, 0);
  log(`Store:       ${TO_STORE}`);
  log(`Definitions: ${totalDefs}`);
  log(`Products:    ${exportedData.products.length}`);
  console.log("");

  // Build all reference maps upfront
  divider("Building reference maps");
  const { sourceImageMap, destImageMap } = await buildImageMaps(log);
  const metaobjectDefMap                 = await buildMetaobjectDefMap(log);

  // Import definitions first
  divider("Importing definitions");
  for (const ownerType of OWNER_TYPES) {
    log(`${ownerType} definitions:`);
    for (const def of exportedData.definitions[ownerType]) {
      try {
        await importDefinition(def, metaobjectDefMap);
        await sleep(300);
      } catch (err) {
        warn(`Failed to create definition "${def.namespace}.${def.key}" (${ownerType}): ${err.message}`);
        summary.definitions.failed++;
        summary.definitions.warnings.push(
          `Definition "${def.namespace}.${def.key}" (${ownerType}): ${err.message}`
        );
      }
    }
  }

  // Import metafield values
  divider("Importing metafield values");
  for (let i = 0; i < exportedData.products.length; i++) {
    const product = exportedData.products[i];
    log(`[${i + 1}/${exportedData.products.length}] ${product.handle}`);
    await importProductMetafields(product, sourceImageMap, destImageMap, sourceEntryMap);
    await sleep(300);
  }

  printSummary();
}

async function runTransfer() {
  const data = await runExport();
  await runImport(data);
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
Shopify Metafield Transfer

Usage:
  node metafieldTransfer.js export      Export metafields from source → metafields_export.json
  node metafieldTransfer.js import      Import metafields from metafields_export.json → dest
  node metafieldTransfer.js transfer    Do both in one step

Config (.env):
  FROM_STORE_URL / FROM_ACCESS_TOKEN
  TO_STORE_URL   / TO_ACCESS_TOKEN

Notes:
  - Run metaobjectTransfer.js export before running this script
  - Image references resolved by filename via lib/imageMap.js
  - Metaobject definition GIDs in validations resolved via lib/metaobjectDefMap.js
  - Metaobject entry GIDs in values resolved via metaobjects_export.json
  - Products matched by handle, variants by title
`);
}