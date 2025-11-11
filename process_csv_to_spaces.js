/**
 * process_csv_to_spaces.js (Realtime Batch Export + CSV_START_LINE + CSV_END_LINE)
 * - Export theo batch ngay khi đủ EXPORT_BATCH_SIZE
 * - Hỗ trợ giới hạn dòng xử lý từ CSV_START_LINE đến CSV_END_LINE
 * - Log, EXIF, Description, và export file chính xác theo dòng thật
 */

const fs = require("fs");
const path = require("path");
const { parse } = require("csv-parse/sync");
const { stringify } = require("csv-stringify/sync");
const fetch = require("node-fetch");
const sharp = require("sharp");
const { exiftool } = require("exiftool-vendored");
const dayjs = require("dayjs");
const { Mutex } = require("async-mutex");
require("dotenv").config();

const pLimit = require("p-limit").default;
const { uploadAuto } = require("./upload");

// ====== CONFIG ======
const INPUT_DIR = path.resolve("./inputs");
const OUTPUT_DIR = path.resolve("./outputs");
const TEMP_DIR = path.resolve("./tmp_imgs");
const OUT_DIR = path.resolve("./processed_imgs");

[INPUT_DIR, OUTPUT_DIR, TEMP_DIR, OUT_DIR].forEach((dir) => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

const CONCURRENCY_PRODUCTS = parseInt(process.env.CONCURRENCY_PRODUCTS || "8", 10);
const CONCURRENCY_IMAGES = parseInt(process.env.CONCURRENCY_IMAGES || "8", 10);
const RANDOM_SKU_ENABLED = (process.env.RANDOM_SKU_ENABLED || "false").toLowerCase() === "true";
const DEBUG_IMAGES = (process.env.DEBUG_IMAGES || "true").toLowerCase() === "true";
const CSV_START_LINE = parseInt(process.env.CSV_START_LINE || "1", 10);
const CSV_END_LINE = parseInt(process.env.CSV_END_LINE || "0", 10); // 0 = không giới hạn

const LAT = parseFloat(process.env.META_LAT || "32.7688");
const LNG = parseFloat(process.env.META_LNG || "-97.3093");
const DAYS_AGO = parseInt(process.env.META_DAYS_AGO || "7", 10);
const AUTHOR_DEFAULT = process.env.META_AUTHOR || "unknown-source.com";
const BUCKET = process.env.S3_BUCKET;
const REGION = process.env.S3_REGION;
const ENDPOINT = process.env.S3_ENDPOINT;
const ACCESS_KEY = process.env.S3_ACCESS_KEY;
const SECRET_KEY = process.env.S3_SECRET_KEY;
const S3_FOLDER = process.env.S3_FOLDER || "";
const EXPORT_BATCH_SIZE = parseInt(process.env.EXPORT_BATCH_SIZE || "1000", 10);

// ====== UTILS ======
const capitalizeWords = (s) => s.replace(/\w\S*/g, (t) => t[0].toUpperCase() + t.slice(1).toLowerCase());
const safeName = (s) => s.replace(/[^a-z0-9]+/gi, "_").replace(/^_+|_+$/g, "").toLowerCase();
const metadataMutex = new Mutex();

async function downloadToTemp(url, baseName, realRow = null) {
  if (DEBUG_IMAGES) {
    if (realRow !== null) console.log(`Row ${realRow} 🖼️ Downloading image: ${url}`);
    else console.log(`🖼️ Downloading image: ${url}`);
  }

  const headers = url.includes("digitaloceanspaces.com") || url.includes("amazonaws.com")
    ? {}
    : { "User-Agent": "Mozilla/5.0", Referer: url };

  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`Download failed: ${url} -> ${res.status}`);
  const buf = await res.buffer();
  const ext = (path.extname(new URL(url).pathname) || ".jpg").split("?")[0];
  const tempPath = path.join(TEMP_DIR, `${baseName}${ext}`);
  fs.writeFileSync(tempPath, buf);
  return tempPath;
}

async function reencode800jpg(inputPath, finalBaseName) {
  let resizedBuffer;
  try {
    resizedBuffer = await sharp(inputPath)
      .toFormat("jpeg")
      .resize({ width: 800, height: 800, fit: "inside" })
      .ensureAlpha()
      .toBuffer();
  } catch (err) {
    console.warn(`⚠️ Sharp failed (${err.message}) — trying fallback`);
    const rawBuffer = fs.readFileSync(inputPath);
    try {
      resizedBuffer = await sharp(rawBuffer)
        .resize({ width: 800, height: 800, fit: "inside" })
        .jpeg({ quality: 90 })
        .toBuffer();
    } catch {
      throw new Error("Unsupported or corrupted image format");
    }
  }

  const outPath = path.join(OUT_DIR, `${finalBaseName}.jpg`);
  await sharp(resizedBuffer).removeAlpha().jpeg({ quality: 90 }).toFile(outPath);
  return outPath;
}

async function setExif(filePath, { title, author, lat = LAT, lng = LNG, daysAgo = DAYS_AGO }) {
  const tags = (title || "").split(/\s+/).filter(Boolean);
  const dateTaken = dayjs().subtract(daysAgo, "day").format("YYYY:MM:DD HH:mm:ss");
  await metadataMutex.runExclusive(async () => {
    await exiftool.write(filePath, {
      Title: title,
      Subject: title,
      Keywords: tags,
      Author: author,
      DateTimeOriginal: dateTaken,
      GPSLatitude: Math.abs(lat),
      GPSLatitudeRef: lat >= 0 ? "N" : "S",
      GPSLongitude: Math.abs(lng),
      GPSLongitudeRef: lng >= 0 ? "E" : "W",
      Copyright: `© ${new Date().getFullYear()} ${author}`,
    });
  });
}

// Bóc link ảnh từ <img> (src, data-src) và URL thuần
function parseImageList(value) {
  if (!value) return [];
  let links = [];

  const htmlMatches = [...value.matchAll(/<img[^>]+>/gi)];
  for (const tag of htmlMatches) {
    const src = tag[0].match(/src=["']([^"']+)["']/i);
    const dataSrc = tag[0].match(/data-src=["']([^"']+)["']/i);
    const url = src?.[1] || dataSrc?.[1];
    if (url && /^https?:\/\//i.test(url)) links.push(url);
  }

  const plainUrls = [...value.matchAll(/https?:\/\/[^\s"'<>]+?\.(?:jpg|jpeg|png|webp|gif|avif)/gi)];
  links.push(...plainUrls.map((m) => m[0]));

  return [...new Set(links)];
}

// ====== CORE ======
async function processOneImage(originalUrl, productTitle, indexInProduct = 0, realRow = null) {
  const author = AUTHOR_DEFAULT || new URL(originalUrl).hostname;
  const cleanTitle = capitalizeWords(productTitle || "Product Image");
  const baseName = safeName(`${cleanTitle}_${indexInProduct + 1}`);
  const tempPath = await downloadToTemp(originalUrl, `${baseName}_raw`, realRow);
  const jpgPath = await reencode800jpg(tempPath, baseName);
  try { fs.unlinkSync(tempPath); } catch {}
  await setExif(jpgPath, { title: cleanTitle, author });

  const uploadedUrl = await uploadAuto(jpgPath, BUCKET, REGION, ACCESS_KEY, SECRET_KEY, ENDPOINT, S3_FOLDER, author);
  if (!uploadedUrl) throw new Error(`Upload failed for ${jpgPath}`);
  return uploadedUrl;
}

// ====== PROCESS CSV FILE ======
async function processCsvFile(inputCsvPath) {
  const fileName = path.basename(inputCsvPath);
  console.log(`🚀 Processing: ${fileName}`);

  const raw = fs.readFileSync(inputCsvPath, "utf8");
  let records = parse(raw, { columns: true, skip_empty_lines: true });

  // Bỏ qua dòng trước CSV_START_LINE
  if (CSV_START_LINE > 1) {
    console.log(`⏭️ Skipping ${CSV_START_LINE - 1} rows — starting from line ${CSV_START_LINE}`);
    records = records.slice(CSV_START_LINE - 1);
  }

  // Giới hạn CSV_END_LINE
  if (CSV_END_LINE > 0) {
    const limit = CSV_END_LINE - CSV_START_LINE + 1;
    records = records.slice(0, limit);
    console.log(`📏 Limiting to lines ${CSV_START_LINE}–${CSV_END_LINE} (${records.length} rows)`);
  }

  if (!records.length) return console.warn(`⚠️ No rows found in ${fileName}`);

  const headers = Object.keys(records[0]).reduce((map, key) => {
    map[key.toLowerCase().trim()] = key;
    return map;
  }, {});
  const imagesCol = headers["images"];
  const titleCol = headers["title"] || headers["name"];
  const descCol = headers["description"];
  if (!imagesCol || !titleCol) {
    console.error(`❌ ${fileName} missing Images or Name/Title column`);
    return;
  }

  const limitProduct = pLimit(CONCURRENCY_PRODUCTS);
  const limitImage = pLimit(CONCURRENCY_IMAGES);
  const batchBuffer = [];
  let batchStart = CSV_START_LINE;
  const shortAuthor = (AUTHOR_DEFAULT || "unk").substring(0, 3);

  // ====== EXPORT BATCH ======
  async function exportBatchIfNeeded(force = false) {
    if (batchBuffer.length >= EXPORT_BATCH_SIZE || (force && batchBuffer.length > 0)) {
      const start = batchStart;
      const end = start + batchBuffer.length - 1;

      const outPath = path.join(
        OUTPUT_DIR,
        `${path.basename(fileName, path.extname(fileName))}_${start}-${end}_output-${shortAuthor}.csv`
      );

      fs.writeFileSync(outPath, stringify(batchBuffer, { header: true }), "utf8");
      console.log(`💾 Exported batch ${start}-${end} (${batchBuffer.length} rows) → ${outPath}`);

      batchStart = end + 1;
      batchBuffer.length = 0;
    }
  }

  // ====== PROCESS ONE ROW ======
  async function processRow(row, i) {
    const title = String(row[titleCol] || "").trim();
    const imageUrls = parseImageList(row[imagesCol]);
    const realRow = CSV_START_LINE + i;

    if (RANDOM_SKU_ENABLED)
      row["SKU"] = `SKU-${String(realRow).padStart(4, "0")}-${Date.now().toString(36)}-${Math.random()
        .toString(36)
        .substring(2, 6)}`;

    const uploadedUrls = [];
    for (const [idx, url] of imageUrls.entries()) {
      try {
        const result = await limitImage(() => processOneImage(url, title, idx, realRow));
        uploadedUrls.push(result);
      } catch (err) {
        console.warn(`⚠️ Image ${idx + 1} failed for Row ${realRow} "${title}": ${err.message}`);
      }
    }

    row[imagesCol] = uploadedUrls.length > 0 ? uploadedUrls.join(", ") : "";

    // ✅ xử lý ảnh trong Description
    if (descCol) {
      const descValue = String(row[descCol] || "").trim();
      const descImageUrls = parseImageList(descValue);
      if (descImageUrls.length > 0) {
        console.log(`📝 Row ${realRow}: found ${descImageUrls.length} image(s) in Description`);
        const uploadedDescUrls = [];
        const failedDescUrls = [];

        for (const [idx, url] of descImageUrls.entries()) {
          try {
            const result = await limitImage(() => processOneImage(url, title, idx, realRow));
            uploadedDescUrls.push({ old: url, new: result });
          } catch (err) {
            console.warn(`⚠️ Desc Image ${idx + 1} failed for Row ${realRow}: ${err.message}`);
            failedDescUrls.push(url);
          }
        }

        let updatedDesc = descValue;
        uploadedDescUrls.forEach(({ old, new: newUrl }) => {
          const escapedOld = old.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
          updatedDesc = updatedDesc.replace(new RegExp(escapedOld, "gi"), newUrl);
        });
        failedDescUrls.forEach((badUrl) => {
          updatedDesc = updatedDesc.replace(
            new RegExp(`<img[^>]+src=["']${badUrl.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}["'][^>]*>`, "gi"),
            ""
          );
          updatedDesc = updatedDesc.replaceAll(badUrl, "");
        });
        row[descCol] = updatedDesc;
      }
    }

    batchBuffer.push(row);
    await exportBatchIfNeeded();
  }

  await Promise.all(records.map((row, i) => limitProduct(() => processRow(row, i))));
  await exportBatchIfNeeded(true);

  console.log(`✅ Finished ${fileName}`);
}

// ====== MAIN ======
(async () => {
  const csvFiles = fs.readdirSync(INPUT_DIR).filter((f) => f.toLowerCase().endsWith(".csv"));
  if (!csvFiles.length) return console.error("❌ No CSV files found in inputs/ folder.");

  for (const file of csvFiles) {
    try {
      await processCsvFile(path.join(INPUT_DIR, file));
    } catch (e) {
      console.error(`❌ Failed file ${file}:`, e.message);
    }
  }

  await exiftool.end();
})();
