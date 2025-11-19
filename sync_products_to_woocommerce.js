/**
 * sync_products_to_woocommerce.js (v3)
 * - Đọc CSV trong /products_to_be_created
 * - Tự tạo Category/Tag nếu thiếu
 * - Chiến lược B: update nếu SKU đã tồn tại
 * - Xử lý lỗi "the SKU ... is already under processing" bằng retry + chuyển sang UPDATE
 * - Chuẩn hoá stock_status / backorders
 * - Xong file thì move sang /done
 */

const fs = require("fs");
const path = require("path");
const { parse } = require("csv-parse/sync");
const axios = require("axios");
const pLimit = require("p-limit").default;
require("dotenv").config();

// ===== Paths
const ROOT_DIR = path.resolve("./products_to_be_created");
const DONE_DIR = path.join(ROOT_DIR, "done");
if (!fs.existsSync(ROOT_DIR)) fs.mkdirSync(ROOT_DIR);
if (!fs.existsSync(DONE_DIR)) fs.mkdirSync(DONE_DIR);

// ===== Env
const CONCURRENCY_PRODUCTS = parseInt(process.env.CONCURRENCY_PRODUCTS || "1", 10); // mặc định 1 cho chắc
const AUTHOR_DEFAULT = process.env.META_AUTHOR || "unknown-source.com";
const WOOCOMMERCE_URL = process.env.WOOCOMMERCE_URL || (AUTHOR_DEFAULT ? `https://${AUTHOR_DEFAULT}` : null);
const WOOCOMMERCE_KEY = process.env.WOOCOMMERCE_KEY;
const WOOCOMMERCE_SECRET = process.env.WOOCOMMERCE_SECRET;
const WOOCOMMERCE_STATUS = (process.env.WOOCOMMERCE_STATUS || "publish").toLowerCase();

if (!WOOCOMMERCE_URL || !WOOCOMMERCE_KEY || !WOOCOMMERCE_SECRET) {
  console.error("❌ Missing WooCommerce credentials in .env");
  process.exit(1);
}

// ===== Utils
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const norm = (s) => String(s || "");
const toLower = (s) => norm(s).toLowerCase().trim();
const splitList = (s) => norm(s).split(",").map((x) => x.trim()).filter(Boolean);
const moneyStr = (v) => {
  const s = norm(v).replace(/[^\d.,-]/g, "");
  if (!s) return undefined;
  const fixed = s.includes(",") && !s.includes(".") ? s.replace(",", ".") : s.replace(/,/g, "");
  return /^\-?\d+(\.\d+)?$/.test(fixed) ? fixed : undefined;
};
function makeSlug(text) {
  const base = norm(text)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  const rnd = Math.random().toString(36).slice(2, 7);
  return base ? `${base}-${rnd}` : rnd;
}

// Attributes & meta
function parseAttributes(row) {
  const attributes = [];
  for (const key of Object.keys(row)) {
    const lk = toLower(key);
    let name = null;
    if (lk.startsWith("attr:")) name = key.slice(5);
    else if (lk.startsWith("attribute_")) name = key.slice(10);
    else if (lk.startsWith("pa_")) name = key; // taxonomy attribute
    if (!name) continue;
    const options = splitList(row[key]);
    if (!options.length) continue;
    const isTax = lk.startsWith("pa_");
    attributes.push({
      name: isTax ? undefined : name,
      slug: isTax ? lk : undefined,
      visible: true,
      variation: false,
      options,
    });
  }
  return attributes;
}
function parseMeta(row) {
  const meta = [];
  for (const key of Object.keys(row)) {
    if (toLower(key).startsWith("meta:")) {
      meta.push({ key: key.slice(5), value: norm(row[key]) });
    }
  }
  return meta;
}

// Stock normalizers
function normalizeStockStatus(val) {
  const s = toLower(val);
  if (["instock", "in stock", "available", "yes", "1", "true"].includes(s)) return "instock";
  if (["outofstock", "out of stock", "soldout", "no", "0", "false"].includes(s)) return "outofstock";
  if (["onbackorder", "backorder"].includes(s)) return "onbackorder";
  return undefined;
}
function normalizeBackorders(val) {
  const s = toLower(val);
  if (["no", "none", "false", "0"].includes(s)) return "no";
  if (["notify", "warn"].includes(s)) return "notify";
  if (["yes", "allow", "1", "true"].includes(s)) return "yes";
  return undefined;
}

// ===== Woo caches
const categoryCache = new Map();
const tagCache = new Map();

async function getCategoryIdByName(name) {
  const key = name.toLowerCase();
  if (categoryCache.has(key)) return categoryCache.get(key);
  try {
    const { data } = await axios.get(`${WOOCOMMERCE_URL}/wp-json/wc/v3/products/categories`, {
      auth: { username: WOOCOMMERCE_KEY, password: WOOCOMMERCE_SECRET },
      params: { search: name },
    });
    if (Array.isArray(data) && data.length > 0) {
      const id = data[0].id;
      categoryCache.set(key, id);
      return id;
    }
    const res = await axios.post(
      `${WOOCOMMERCE_URL}/wp-json/wc/v3/products/categories`,
      { name },
      { auth: { username: WOOCOMMERCE_KEY, password: WOOCOMMERCE_SECRET } }
    );
    categoryCache.set(key, res.data.id);
    console.log(`📂 Created new category: ${name} (ID ${res.data.id})`);
    return res.data.id;
  } catch (err) {
    console.warn(`⚠️ Category error "${name}": ${err.response?.data?.message || err.message}`);
    return null;
  }
}
async function getTagIdByName(name) {
  const key = name.toLowerCase();
  if (tagCache.has(key)) return tagCache.get(key);
  try {
    const { data } = await axios.get(`${WOOCOMMERCE_URL}/wp-json/wc/v3/products/tags`, {
      auth: { username: WOOCOMMERCE_KEY, password: WOOCOMMERCE_SECRET },
      params: { search: name },
    });
    if (Array.isArray(data) && data.length > 0) {
      const id = data[0].id;
      tagCache.set(key, id);
      return id;
    }
    const res = await axios.post(
      `${WOOCOMMERCE_URL}/wp-json/wc/v3/products/tags`,
      { name },
      { auth: { username: WOOCOMMERCE_KEY, password: WOOCOMMERCE_SECRET } }
    );
    tagCache.set(key, res.data.id);
    console.log(`🏷️ Created new tag: ${name} (ID ${res.data.id})`);
    return res.data.id;
  } catch (err) {
    console.warn(`⚠️ Tag error "${name}": ${err.response?.data?.message || err.message}`);
    return null;
  }
}

// ===== SKU helpers
async function getProductBySKU(sku) {
  if (!sku) return null;
  try {
    const { data } = await axios.get(`${WOOCOMMERCE_URL}/wp-json/wc/v3/products`, {
      auth: { username: WOOCOMMERCE_KEY, password: WOOCOMMERCE_SECRET },
      params: { sku },
    });
    if (Array.isArray(data) && data.length > 0) return data[0];
    return null;
  } catch (err) {
    console.warn(`⚠️ Error checking SKU ${sku}: ${err.response?.data?.message || err.message}`);
    return null;
  }
}
function isUnderProcessingError(err) {
  const msg = err?.response?.data?.message || err?.message || "";
  return /under processing/i.test(msg) || /already exists/i.test(msg) || /duplicate/i.test(msg);
}
async function createOrUpdateWithRetry(product) {
  // 1) nếu đã có → update
  const existing = await getProductBySKU(product.sku);
  if (existing) {
    const res = await axios.put(`${WOOCOMMERCE_URL}/wp-json/wc/v3/products/${existing.id}`, product, {
      auth: { username: WOOCOMMERCE_KEY, password: WOOCOMMERCE_SECRET },
    });
    console.log(`🔄 Updated: ${res.data.name} (ID: ${res.data.id})`);
    return;
  }

  // 2) thử tạo mới
  try {
    const res = await axios.post(`${WOOCOMMERCE_URL}/wp-json/wc/v3/products`, product, {
      auth: { username: WOOCOMMERCE_KEY, password: WOOCOMMERCE_SECRET },
    });
    console.log(`✅ Created: ${res.data.name} (ID: ${res.data.id})`);
    return;
  } catch (err) {
    if (!isUnderProcessingError(err)) throw err;
    console.warn(`⏳ SKU ${product.sku} under processing — will retry as UPDATE...`);
  }

  // 3) Retry chu kỳ: 2s → 4s → 6s → 10s
  const waits = [2000, 4000, 6000, 10000];
  for (let i = 0; i < waits.length; i++) {
    await sleep(waits[i]);
    const found = await getProductBySKU(product.sku);
    if (found) {
      try {
        const res = await axios.put(`${WOOCOMMERCE_URL}/wp-json/wc/v3/products/${found.id}`, product, {
          auth: { username: WOOCOMMERCE_KEY, password: WOOCOMMERCE_SECRET },
        });
        console.log(`🔁 Retried OK (${(waits[i] / 1000)}s): ${res.data.name} (ID: ${res.data.id})`);
        return;
      } catch (e2) {
        if (!isUnderProcessingError(e2)) throw e2;
        // vẫn dưới processing → tiếp tục vòng lặp
      }
    }
  }

  // 4) Lần cuối: thử GET lần nữa rồi quyết định
  const last = await getProductBySKU(product.sku);
  if (last) {
    const res = await axios.put(`${WOOCOMMERCE_URL}/wp-json/wc/v3/products/${last.id}`, product, {
      auth: { username: WOOCOMMERCE_KEY, password: WOOCOMMERCE_SECRET },
    });
    console.log(`🔁 Retried OK (final): ${res.data.name} (ID: ${res.data.id})`);
    return;
  }
  // Nếu tới đây vẫn không được, ném lỗi để log ra ngoài
  throw new Error(`SKU ${product.sku} is still under processing after retries`);
}

// ===== Main product builder
async function createWooProduct(row) {
  // categories & tags
  let categories = [];
  let tags = [];
  if (row["Categories"]) {
    const ids = (await Promise.all(splitList(row["Categories"]).map((n) => getCategoryIdByName(n)))).filter(Boolean);
    categories = ids.map((id) => ({ id }));
  }
  if (row["Tags"]) {
    const ids = (await Promise.all(splitList(row["Tags"]).map((n) => getTagIdByName(n)))).filter(Boolean);
    tags = ids.map((id) => ({ id }));
  }

  const product = {
    name: row["Name"] || row["Title"] || "Untitled Product",
    slug: makeSlug(row["Name"] || row["Title"] || "Untitled Product"),
    type: "simple",
    status: WOOCOMMERCE_STATUS,
    description: row["Description"] || "",
    short_description: row["Short description"] || "",
    sku: (row["SKU"] || "").trim(),
    regular_price: moneyStr(row["Regular price"] || row["Price"]),
    sale_price: moneyStr(row["Sale price"]),
    tax_status: toLower(row["Tax status"]),
    tax_class: row["Tax class"],
    manage_stock: ["true", "yes", "1"].includes(toLower(row["Manage stock"])),
    stock_quantity: row["Stock quantity"] ? Number(row["Stock quantity"]) : undefined,
    stock_status: normalizeStockStatus(row["Stock status"]),
    backorders: normalizeBackorders(row["Backorders"]),
    weight: row["Weight"],
    dimensions:
      row["Length"] || row["Width"] || row["Height"]
        ? { length: row["Length"] || "", width: row["Width"] || "", height: row["Height"] || "" }
        : undefined,
    shipping_class: row["Shipping class"],
    categories,
    tags,
    images: row["Images"] ? splitList(row["Images"]).map((u) => ({ src: u })) : undefined,
    attributes: parseAttributes(row),
    meta_data: parseMeta(row),
  };

  // cleanup undefined
  Object.keys(product).forEach((k) => (product[k] === undefined ? delete product[k] : 0));
  if (product.dimensions && !Object.values(product.dimensions).some((v) => v))
    delete product.dimensions;

  try {
    await createOrUpdateWithRetry(product);
  } catch (err) {
    console.warn(`⚠️ Failed for ${product.name}: ${err.response?.data?.message || err.message}`);
  }
}

// ===== Process one CSV
async function processCsvFile(filePath) {
  console.log(`🚀 Syncing products from: ${path.basename(filePath)}`);
  const rows = parse(fs.readFileSync(filePath, "utf8"), { columns: true, skip_empty_lines: true });

  const limit = pLimit(CONCURRENCY_PRODUCTS);
  await Promise.all(rows.map((r) => limit(() => createWooProduct(r))));
  console.log(`✅ Done: ${rows.length} products from ${path.basename(filePath)}`);

  const dest = path.join(DONE_DIR, path.basename(filePath));
  // fs.renameSync(filePath, dest);
  console.log(`📦 Moved file to done/: ${path.basename(filePath)}`);
}

// ===== Main
(async () => {
  const files = fs.readdirSync(ROOT_DIR).filter((f) => f.toLowerCase().endsWith(".csv"));
  if (!files.length) {
    console.log("📂 No CSV files found in products_to_be_created/");
    return;
  }
  console.log(`🧩 Found ${files.length} CSV file(s):`);
  files.forEach((f, i) => console.log(`  ${i + 1}. ${f}`));

  for (const f of files) {
    try {
      await processCsvFile(path.join(ROOT_DIR, f));
    } catch (e) {
      console.error(`❌ Failed ${f}: ${e.message}`);
    }
  }
  console.log("✨ All done!");
})();
