/**
 * metaobjectTransfer.js
 *
 * Transfers metaobject definitions and entries from one Shopify store to another.
 * Skips definitions/entries that already exist in the destination store.
 * Resolves image and metaobject cross-references by filename and handle.
 *
 * Usage:
 *   node metaobjectTransfer.js export
 *   node metaobjectTransfer.js import
 *   node metaobjectTransfer.js transfer
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import * as dotenv from "dotenv";
import { buildImageMaps } from "./lib/imageMap.js";

dotenv.config();

const __dirname   = path.dirname(fileURLToPath(import.meta.url));
const EXPORT_FILE = path.join(__dirname, "metaobjects_export.json");
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

// ─── Summary ──────────────────────────────────────────────────────────────────
const summary = {
  definitions: { created: 0, skipped: 0, failed: 0, warnings: [] },
  entries:     { created: 0, skipped: 0, failed: 0, warnings: [] },
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

  Entries
    Created:  ${summary.entries.created}
    Skipped:  ${summary.entries.skipped}
    Failed:   ${summary.entries.failed}
`);

  const allWarnings = [
    ...summary.definitions.warnings,
    ...summary.entries.warnings,
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
function getGidType(gid) {
  if (!gid || typeof gid !== "string") return null;
  const match = gid.match(/^gid:\/\/shopify\/(\w+)\//);
  return match ? match[1] : null;
}

function isGid(value) {
  return typeof value === "string" && value.startsWith("gid://shopify/");
}

// ─── Metaobject GID cache + resolver ─────────────────────────────────────────
const metaobjectGidCache = {};

const GET_METAOBJECT_BY_HANDLE_QUERY = `
  query GetMetaobjectByHandle($type: String!, $handle: String!) {
    metaobjectByHandle(handle: { type: $type, handle: $handle }) {
      id
      handle
    }
  }
`;

async function resolveMetaobjectGid(type, handle) {
  const cacheKey = `${type}:${handle}`;
  if (metaobjectGidCache[cacheKey]) return metaobjectGidCache[cacheKey];

  const data = await gql(TO_STORE, TO_TOKEN, GET_METAOBJECT_BY_HANDLE_QUERY, { type, handle });
  const gid  = data.metaobjectByHandle?.id ?? null;

  if (gid) metaobjectGidCache[cacheKey] = gid;
  return gid;
}

// ─── Reference resolver ───────────────────────────────────────────────────────
async function resolveGid(sourceGid, sourceImageMap, destImageMap, exportedData) {
  const type = getGidType(sourceGid);

  if (type === "MediaImage") {
    const filename = sourceImageMap[sourceGid];
    if (!filename) {
      return { resolved: null, warning: `No filename found for source image GID ${sourceGid}` };
    }
    const destGid = destImageMap[filename];
    if (!destGid) {
      return {
        resolved: null,
        warning: `Image "${filename}" not found in destination store — run fileTransfer.js first`,
      };
    }
    return { resolved: destGid, warning: null };
  }

  if (type === "Metaobject") {
    for (const { definition, entries } of exportedData) {
      const entry = entries.find((e) => e.id === sourceGid);
      if (entry) {
        const destGid = await resolveMetaobjectGid(definition.type, entry.handle);
        if (!destGid) {
          return {
            resolved: null,
            warning: `Metaobject "${entry.handle}" (${definition.type}) not found in destination store`,
          };
        }
        return { resolved: destGid, warning: null };
      }
    }
    return { resolved: null, warning: `Could not find exported metaobject for source GID ${sourceGid}` };
  }

  return { resolved: null, warning: `Unresolved GID type "${type}" for value ${sourceGid}` };
}

async function resolveFieldValue(field, sourceImageMap, destImageMap, exportedData) {
  const { value } = field;
  const warnings  = [];

  if (!value) return { resolvedValue: value, warnings };

  // Single GID reference
  if (isGid(value)) {
    const { resolved, warning } = await resolveGid(value, sourceImageMap, destImageMap, exportedData);
    if (warning) warnings.push(warning);
    return { resolvedValue: resolved ?? value, warnings };
  }

  // JSON array of GID references
  if (value.startsWith("[")) {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed) && parsed.every(isGid)) {
        const resolved = [];
        for (const gid of parsed) {
          const { resolved: destGid, warning } = await resolveGid(
            gid, sourceImageMap, destImageMap, exportedData
          );
          if (warning) warnings.push(warning);
          resolved.push(destGid ?? gid);
        }
        return { resolvedValue: JSON.stringify(resolved), warnings };
      }
    } catch { /* not a JSON array, fall through */ }
  }

  return { resolvedValue: value, warnings };
}

// ─── Dependency ordering ──────────────────────────────────────────────────────
function getReferencedTypes(definitionData, allExportedData) {
  const referencedTypes = new Set();

  for (const entry of definitionData.entries) {
    for (const field of entry.fields) {
      if (!field.value) continue;

      const gids = [];
      if (isGid(field.value)) {
        gids.push(field.value);
      } else if (field.value.startsWith("[")) {
        try {
          const parsed = JSON.parse(field.value);
          if (Array.isArray(parsed)) parsed.filter(isGid).forEach((g) => gids.push(g));
        } catch { /* not JSON */ }
      }

      for (const gid of gids) {
        if (getGidType(gid) === "Metaobject") {
          for (const { definition, entries } of allExportedData) {
            if (entries.some((e) => e.id === gid)) {
              referencedTypes.add(definition.type);
            }
          }
        }
      }
    }
  }

  return referencedTypes;
}

function sortByDependencies(exportedData) {
  const sorted  = [];
  const visited = new Set();

  function visit(type) {
    if (visited.has(type)) return;
    visited.add(type);

    const defData = exportedData.find((d) => d.definition.type === type);
    if (!defData) return;

    const deps = getReferencedTypes(defData, exportedData);
    for (const dep of deps) visit(dep);

    sorted.push(defData);
  }

  for (const defData of exportedData) visit(defData.definition.type);

  return sorted;
}

// ─── EXPORT ───────────────────────────────────────────────────────────────────
const DEFINITIONS_QUERY = `
  query GetMetaobjectDefinitions($cursor: String) {
    metaobjectDefinitions(first: 50, after: $cursor) {
      edges {
        cursor
        node {
          id
          type
          name
          description
          displayNameKey
          access { admin storefront }
          capabilities {
            publishable { enabled }
            translatable { enabled }
            renderable {
              enabled
              data { metaTitleKey metaDescriptionKey }
            }
            onlineStore {
              enabled
              data { urlHandle }
            }
          }
          fieldDefinitions {
            key
            name
            description
            required
            type { name }
            validations { name value }
          }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

const ENTRIES_QUERY = `
  query GetMetaobjects($type: String!, $cursor: String) {
    metaobjects(type: $type, first: 50, after: $cursor) {
      edges {
        cursor
        node {
          id
          handle
          displayName
          fields { key value type }
          capabilities {
            publishable { status }
          }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

async function fetchAllDefinitions() {
  const definitions = [];
  let cursor = null;

  while (true) {
    const data  = await gql(FROM_STORE, FROM_TOKEN, DEFINITIONS_QUERY, { cursor });
    const edges = data.metaobjectDefinitions.edges;
    definitions.push(...edges.map((e) => e.node));
    if (!data.metaobjectDefinitions.pageInfo.hasNextPage) break;
    cursor = data.metaobjectDefinitions.pageInfo.endCursor;
    await sleep(300);
  }

  return definitions;
}

async function fetchAllEntries(type) {
  const entries = [];
  let cursor = null;

  while (true) {
    const data  = await gql(FROM_STORE, FROM_TOKEN, ENTRIES_QUERY, { type, cursor });
    const edges = data.metaobjects.edges;
    entries.push(...edges.map((e) => e.node));
    if (!data.metaobjects.pageInfo.hasNextPage) break;
    cursor = data.metaobjects.pageInfo.endCursor;
    await sleep(300);
  }

  return entries;
}

async function runExport() {
  divider("EXPORT — Fetching metaobjects from source store");

  if (!FROM_STORE || !FROM_TOKEN) fail("Missing FROM_STORE_URL or FROM_ACCESS_TOKEN in .env");

  log(`Store: ${FROM_STORE}`);
  log("Fetching definitions...");

  const definitions = await fetchAllDefinitions();
  log(`Found ${definitions.length} definitions`);

  const result = [];

  for (const def of definitions) {
    log(`Fetching entries for: ${def.type}`);
    const entries = await fetchAllEntries(def.type);
    log(`  → ${entries.length} entries`);
    result.push({ definition: def, entries });
  }

  fs.writeFileSync(EXPORT_FILE, JSON.stringify(result, null, 2));
  ok(`Exported ${definitions.length} definitions → metaobjects_export.json`);

  return result;
}

// ─── IMPORT ───────────────────────────────────────────────────────────────────
const GET_DEFINITION_BY_TYPE_QUERY = `
  query GetDefinitionByType($type: String!) {
    metaobjectDefinitionByType(type: $type) {
      id
      type
      fieldDefinitions {
        key
        type { name }
        required
      }
    }
  }
`;

const GET_ENTRY_BY_HANDLE_QUERY = `
  query GetEntryByHandle($type: String!, $handle: String!) {
    metaobjectByHandle(handle: { type: $type, handle: $handle }) {
      id
      handle
    }
  }
`;

const CREATE_DEFINITION_MUTATION = `
  mutation CreateMetaobjectDefinition($definition: MetaobjectDefinitionCreateInput!) {
    metaobjectDefinitionCreate(definition: $definition) {
      metaobjectDefinition { id type }
      userErrors { field message code }
    }
  }
`;

const CREATE_ENTRY_MUTATION = `
  mutation CreateMetaobject($metaobject: MetaobjectCreateInput!) {
    metaobjectCreate(metaobject: $metaobject) {
      metaobject { id handle }
      userErrors { field message code }
    }
  }
`;

function buildDefinitionInput(def) {
  const input = {
    type:             def.type,
    name:             def.name,
    description:      def.description,
    displayNameKey:   def.displayNameKey,
    fieldDefinitions: def.fieldDefinitions.map((f) => ({
      key:         f.key,
      name:        f.name,
      description: f.description,
      required:    f.required,
      type:        f.type.name,
      validations: f.validations?.length ? f.validations : undefined,
    })),
  };

  if (def.access?.admin || def.access?.storefront) {
    input.access = {};
    if (def.access.admin)      input.access.admin      = def.access.admin;
    if (def.access.storefront) input.access.storefront = def.access.storefront;
  }

  const caps = {};
  if (def.capabilities?.publishable?.enabled)  caps.publishable  = { enabled: true };
  if (def.capabilities?.translatable?.enabled) caps.translatable = { enabled: true };
  if (def.capabilities?.renderable?.enabled) {
    caps.renderable = {
      enabled: true,
      data: {
        metaTitleKey:       def.capabilities.renderable.data?.metaTitleKey,
        metaDescriptionKey: def.capabilities.renderable.data?.metaDescriptionKey,
      },
    };
  }
  if (def.capabilities?.onlineStore?.enabled) {
    caps.onlineStore = {
      enabled: true,
      data: { urlHandle: def.capabilities.onlineStore.data?.urlHandle },
    };
  }
  if (Object.keys(caps).length) input.capabilities = caps;

  return input;
}

function validateEntryAgainstDefinition(entry, toDefinition) {
  const warnings   = [];
  const toFieldMap = Object.fromEntries(toDefinition.fieldDefinitions.map((f) => [f.key, f]));

  for (const field of entry.fields) {
    if (!toFieldMap[field.key]) {
      warnings.push(`Entry "${entry.handle}": field "${field.key}" does not exist in destination definition`);
      continue;
    }
    const toField = toFieldMap[field.key];
    if (toField.type.name !== field.type) {
      warnings.push(
        `Entry "${entry.handle}": field "${field.key}" type mismatch — source: ${field.type}, dest: ${toField.type.name}`
      );
    }
  }

  for (const toField of toDefinition.fieldDefinitions) {
    if (toField.required) {
      const hasField = entry.fields.some((f) => f.key === toField.key && f.value !== null);
      if (!hasField) {
        warnings.push(
          `Entry "${entry.handle}": required field "${toField.key}" in destination is missing or null in source`
        );
      }
    }
  }

  return warnings;
}

async function importDefinition(def) {
  const existing = await gql(TO_STORE, TO_TOKEN, GET_DEFINITION_BY_TYPE_QUERY, { type: def.type });

  if (existing.metaobjectDefinitionByType) {
    log(`  Definition "${def.type}" already exists — skipping`);
    summary.definitions.skipped++;
    return existing.metaobjectDefinitionByType;
  }

  const data = await gql(TO_STORE, TO_TOKEN, CREATE_DEFINITION_MUTATION, {
    definition: buildDefinitionInput(def),
  });

  const { metaobjectDefinition, userErrors } = data.metaobjectDefinitionCreate;
  if (userErrors.length) throw new Error(userErrors.map((e) => e.message).join(", "));

  summary.definitions.created++;
  return metaobjectDefinition;
}

async function importEntry(entry, type, toDefinition, sourceImageMap, destImageMap, exportedData) {
  const existing = await gql(TO_STORE, TO_TOKEN, GET_ENTRY_BY_HANDLE_QUERY, {
    type,
    handle: entry.handle,
  });

  if (existing.metaobjectByHandle) {
    log(`    Entry "${entry.handle}" already exists — skipping`);
    summary.entries.skipped++;
    return;
  }

  const validationWarnings = validateEntryAgainstDefinition(entry, toDefinition);
  if (validationWarnings.length) {
    validationWarnings.forEach((w) => {
      warn(w);
      summary.entries.warnings.push(w);
    });
  }

  const validKeys = new Set(toDefinition.fieldDefinitions.map((f) => f.key));
  const fields    = [];

  for (const field of entry.fields) {
    if (!validKeys.has(field.key) || field.value === null) continue;

    const { resolvedValue, warnings: refWarnings } = await resolveFieldValue(
      field, sourceImageMap, destImageMap, exportedData
    );

    if (refWarnings.length) {
      refWarnings.forEach((w) => {
        warn(`  Entry "${entry.handle}" field "${field.key}": ${w}`);
        summary.entries.warnings.push(`Entry "${entry.handle}" field "${field.key}": ${w}`);
      });
    }

    fields.push({ key: field.key, value: resolvedValue });
  }

  const metaobjectInput = { type, handle: entry.handle, fields };

  if (entry.capabilities?.publishable?.status) {
    metaobjectInput.capabilities = {
      publishable: { status: entry.capabilities.publishable.status },
    };
  }

  const data = await gql(TO_STORE, TO_TOKEN, CREATE_ENTRY_MUTATION, {
    metaobject: metaobjectInput,
  });

  const { userErrors } = data.metaobjectCreate;
  if (userErrors.length) throw new Error(userErrors.map((e) => e.message).join(", "));

  summary.entries.created++;
}

async function runImport(exportedData = null) {
  divider("IMPORT — Transferring metaobjects to destination store");

  if (!TO_STORE || !TO_TOKEN) fail("Missing TO_STORE_URL or TO_ACCESS_TOKEN in .env");

  if (!exportedData) {
    if (!fs.existsSync(EXPORT_FILE)) {
      fail("metaobjects_export.json not found. Run `node metaobjectTransfer.js export` first.");
    }
    exportedData = JSON.parse(fs.readFileSync(EXPORT_FILE, "utf-8"));
  }

  log(`Store:       ${TO_STORE}`);
  log(`Definitions: ${exportedData.length}`);
  log(`Entries:     ${exportedData.reduce((acc, d) => acc + d.entries.length, 0)}`);
  console.log("");

  divider("Building image reference maps");
  const { sourceImageMap, destImageMap } = await buildImageMaps(log);

  divider("Resolving definition dependency order");
  const sorted = sortByDependencies(exportedData);
  log(`Order: ${sorted.map((d) => d.definition.type).join(" → ")}`);

  divider("Importing definitions and entries");

  for (const { definition, entries } of sorted) {
    log(`\nProcessing definition: ${definition.type}`);

    let toDefinition;

    try {
      toDefinition = await importDefinition(definition);
      await sleep(300);
    } catch (err) {
      warn(`Failed to create definition "${definition.type}": ${err.message}`);
      summary.definitions.failed++;
      summary.definitions.warnings.push(`Definition "${definition.type}" failed: ${err.message}`);
      continue;
    }

    if (!toDefinition.fieldDefinitions) {
      const data   = await gql(TO_STORE, TO_TOKEN, GET_DEFINITION_BY_TYPE_QUERY, { type: definition.type });
      toDefinition = data.metaobjectDefinitionByType;
    }

    for (const entry of entries) {
      process.stdout.write(`    [entry] ${entry.handle} ... `);
      try {
        await importEntry(entry, definition.type, toDefinition, sourceImageMap, destImageMap, exportedData);
        console.log("✅");
      } catch (err) {
        console.log("❌");
        warn(`Failed: ${err.message}`);
        summary.entries.failed++;
        summary.entries.warnings.push(`Entry "${entry.handle}" (${definition.type}): ${err.message}`);
      }
      await sleep(300);
    }
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
Shopify Metaobject Transfer

Usage:
  node metaobjectTransfer.js export      Export definitions + entries → metaobjects_export.json
  node metaobjectTransfer.js import      Import from metaobjects_export.json → dest store
  node metaobjectTransfer.js transfer    Do both in one step

Config (.env):
  FROM_STORE_URL / FROM_ACCESS_TOKEN
  TO_STORE_URL   / TO_ACCESS_TOKEN

Notes:
  - Image references are resolved by filename — run fileTransfer.js first
  - Metaobject cross-references are resolved by handle
  - Definition import order is automatically sorted by dependencies
`);
}