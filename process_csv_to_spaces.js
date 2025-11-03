/**
 * process_csv_to_spaces.js
 * - Đọc tất cả CSV trong folder inputs/
 * - Với cột `Images`: tải ảnh, thêm metadata, re-encode JPG 800x800, upload lên DO Spaces
 * - Thay cột `Images` bằng link mới, xuất ra folder outputs/ với hậu tố _output.csv
 * - Xuất từng đợt (batch) theo số lượng sản phẩm quy định qua EXPORT_BATCH_SIZE
 * - Tên file xuất theo dạng _1-500, _501-1000, ...
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
// const { uploadToS3 } = require("./upload");
const { uploadAuto } = require("./upload");

// ====== CONFIG ======
const INPUT_DIR = path.resolve("./inputs");
const OUTPUT_DIR = path.resolve("./outputs");
const TEMP_DIR = path.resolve("./tmp_imgs");
const OUT_DIR = path.resolve("./processed_imgs");

if (!fs.existsSync(INPUT_DIR)) fs.mkdirSync(INPUT_DIR, { recursive: true });
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
if (!fs.existsSync(TEMP_DIR)) fs.mkdirSync(TEMP_DIR, { recursive: true });
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

// Concurrency
const CONCURRENCY_PRODUCTS = parseInt(
  process.env.CONCURRENCY_PRODUCTS || process.env.CONCURRENCY || "8",
  10
);
const CONCURRENCY_IMAGES = parseInt(
  process.env.CONCURRENCY_IMAGES || process.env.CONCURRENCY || "8",
  10
);
const RANDOM_SKU_ENABLED = (process.env.RANDOM_SKU_ENABLED || "false").toLowerCase() === "true";

// Metadata defaults
const LAT = parseFloat(process.env.META_LAT || "32.7688");
const LNG = parseFloat(process.env.META_LNG || "-97.3093");
const DAYS_AGO = parseInt(process.env.META_DAYS_AGO || "7", 10);
const AUTHOR_DEFAULT = process.env.META_AUTHOR || "unknown-source.com";
const IMG_NAME_WITH_AUTHOR = process.env.IMG_NAME_WITH_AUTHOR || false;

// Spaces config
const BUCKET = process.env.S3_BUCKET;
const REGION = process.env.S3_REGION;
const ENDPOINT = process.env.S3_ENDPOINT;
const ACCESS_KEY = process.env.S3_ACCESS_KEY;
const SECRET_KEY = process.env.S3_SECRET_KEY;
const S3_FOLDER = process.env.S3_FOLDER || "";

// Batch export config
const EXPORT_BATCH_SIZE = parseInt(process.env.EXPORT_BATCH_SIZE || "1000", 10);

// ====== UTILS ======
const delay = (ms) => new Promise((r) => setTimeout(r, ms));
const capitalizeWords = (s) =>
  s.replace(/\w\S*/g, (t) => t[0].toUpperCase() + t.slice(1).toLowerCase());
const safeName = (s) =>
  s.replace(/[^a-z0-9]+/gi, "_").replace(/^_+|_+$/g, "").toLowerCase();

const metadataMutex = new Mutex();

async function downloadToTemp(url, baseName) {
  console.log(`🖼️ Downloading image: ${url}`);
  const headers =
    url.includes("digitaloceanspaces.com") || url.includes("amazonaws.com")
      ? {}
      : {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        Referer: url,
      };

  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`Download failed: ${url} -> ${res.status}`);
  const buf = await res.buffer();
  const ext = (path.extname(new URL(url).pathname) || ".jpg").split("?")[0];
  const tempPath = path.join(TEMP_DIR, `${baseName}${ext}`);
  fs.writeFileSync(tempPath, buf);
  return tempPath;
}

async function reencode800jpg(inputPath, finalBaseName) {
  const resizedBuffer = await sharp(inputPath)
    .resize({ width: 800, height: 800, fit: "inside" })
    .modulate({ brightness: 1.001 })
    .ensureAlpha()
    .toBuffer();

  const meta = await sharp(resizedBuffer).metadata();
  const padX = Math.floor((800 - (meta.width || 800)) / 2);
  const padY = Math.floor((800 - (meta.height || 800)) / 2);

  const outPath = path.join(OUT_DIR, `${finalBaseName}.jpg`);
  await sharp(resizedBuffer)
    .extend({
      top: padY,
      bottom: 800 - (meta.height || 800) - padY,
      left: padX,
      right: 800 - (meta.width || 800) - padX,
      background: { r: 255, g: 255, b: 255, alpha: 1 },
    })
    .removeAlpha()
    .jpeg({ quality: 90 })
    .toFile(outPath);

  return outPath;
}

async function setExif(filePath, { title, author, lat = LAT, lng = LNG, daysAgo = DAYS_AGO }) {
  const tags = (title || "").split(/\s+/).filter(Boolean);
  const dateTaken = dayjs().subtract(daysAgo, "day").format("YYYY:MM:DD HH:mm:ss");

  await metadataMutex.runExclusive(async () => {
    await exiftool.write(filePath, {
      Title: title,
      Subject: title,
      Rating: 5,
      Keywords: tags,
      Comment: title,
      Author: author,
      XPTitle: title,
      XPSubject: title,
      XPComment: title,
      XPKeywords: tags.join(";"),
      XPAuthor: author,
      DateTimeOriginal: dateTaken,
      GPSLatitude: Math.abs(lat),
      GPSLatitudeRef: lat >= 0 ? "N" : "S",
      GPSLongitude: Math.abs(lng),
      GPSLongitudeRef: lng >= 0 ? "E" : "W",
      Make: `${author}`,
      Model: `${author} Product Image`,
      Copyright: `© ${new Date().getFullYear()} ${author}`,
    });
  });
}

function parseImageList(value) {
  if (!value) return [];
  return value
    .split(/,\s+/)
    .map((s) => s.trim())
    .filter((s) => /^https?:\/\//i.test(s));
}

// ====== CORE ======
async function processOneImage(originalUrl, productTitle, indexInProduct = 0) {
  const author =
    AUTHOR_DEFAULT && AUTHOR_DEFAULT !== "unknown-source.com"
      ? AUTHOR_DEFAULT
      : new URL(originalUrl).hostname;

  const cleanTitle = capitalizeWords(
    productTitle ||
    path.basename(originalUrl).replace(/\.[^/.]+$/, "").replace(/[-_]+/g, " ")
  );
  const baseName = safeName(`${cleanTitle}_${indexInProduct + 1}`);

  const tempPath = await downloadToTemp(originalUrl, `${baseName}_raw`);
  const jpgPath = await reencode800jpg(tempPath, baseName);
  try {
    fs.unlinkSync(tempPath);
  } catch { }

  await setExif(jpgPath, { title: cleanTitle, author });

  const uploadedUrl = await uploadAuto(
    jpgPath,
    BUCKET,
    REGION,
    ACCESS_KEY,
    SECRET_KEY,
    ENDPOINT,
    S3_FOLDER,
    author
  );

  if (!uploadedUrl) throw new Error(`Upload failed for ${jpgPath}`);
  return uploadedUrl;
}

// ====== PROCESS CSV FILE ======
async function processCsvFile(inputCsvPath) {
  const fileName = path.basename(inputCsvPath);
  console.log(`🚀 Processing: ${fileName}`);

  const raw = fs.readFileSync(inputCsvPath, "utf8");
  const records = parse(raw, { columns: true, skip_empty_lines: true });

  if (!records.length) {
    console.warn(`⚠️ No rows found in ${fileName}, skipped.`);
    return;
  }

  const headers = Object.keys(records[0]).reduce((map, key) => {
    map[key.toLowerCase().trim()] = key;
    return map;
  }, {});
  const imagesCol = headers["images"];
  const titleCol = headers["title"] || headers["name"];

  if (!imagesCol || !titleCol) {
    console.error(`❌ ${fileName} missing Images or Name/Title column, skipped.`);
    return;
  }

  const successRows = [];
  const failedRows = [];
  let processedCount = 0;

  async function exportChunkIfNeeded(force = false) {
    if (successRows.length === 0 && failedRows.length === 0) return;

    // Tính khoảng range file hiện tại
    const start = Math.floor((processedCount - successRows.length - failedRows.length) / EXPORT_BATCH_SIZE) * EXPORT_BATCH_SIZE + 1;
    const end = processedCount;

    if (force || processedCount % EXPORT_BATCH_SIZE === 0) {
      const rangeStart = start;
      const rangeEnd = force ? processedCount : start + EXPORT_BATCH_SIZE - 1;
      const shortAuthor = (process.env.META_AUTHOR || "unk").substring(0, 3);

      const successFile = path.join(
        OUTPUT_DIR,
        `${path.basename(fileName, path.extname(fileName))}_${rangeStart}-${rangeEnd}_output-${shortAuthor}.csv`
      );
      const failFile = path.join(
        OUTPUT_DIR,
        `${path.basename(fileName, path.extname(fileName))}_${rangeStart}-${rangeEnd}_failed-${shortAuthor}.csv`
      );

      if (successRows.length > 0) {
        const header = Object.keys(successRows[0]);
        const csvOut = stringify(successRows, { header: true, columns: header });
        fs.writeFileSync(successFile, csvOut, "utf8");
        console.log(`💾 Exported ${rangeStart}-${rangeEnd} (${successRows.length} success) → ${successFile}`);
        successRows.length = 0;
      }

      if (failedRows.length > 0) {
        const header = Object.keys(failedRows[0]);
        const csvFail = stringify(failedRows, { header: true, columns: header });
        fs.writeFileSync(failFile, csvFail, "utf8");
        console.warn(`⚠️ Exported ${rangeStart}-${rangeEnd} (${failedRows.length} failed) → ${failFile}`);
        failedRows.length = 0;
      }
    }
  }

  const limitProduct = pLimit(CONCURRENCY_PRODUCTS);
  const limitImage = pLimit(CONCURRENCY_IMAGES);

  const tasks = records.map((row, i) =>
    limitProduct(async () => {
      const productTitle = String(row[titleCol] || "").trim();
      const imageUrls = parseImageList(row[imagesCol]);
      if (RANDOM_SKU_ENABLED) {
        const randomSku = `SKU-${String(i + 1).padStart(4, "0")}-${Date.now().toString(36).toUpperCase()}-${Math.random().toString(36).substring(2, 6).toUpperCase()}`;
        row["SKU"] = randomSku;
      }

      if (imageUrls.length === 0) {
        console.warn(`[${fileName}] Row ${i + 1}: no valid image URL -> mark as failed`);
        row[imagesCol] = "";
        failedRows.push({ ...row, __error: "No valid image URL" });
        processedCount++;
        await exportChunkIfNeeded();
        return;
      }

      console.log(
        `📦 [${fileName}] Row ${i + 1}/${records.length}: "${productTitle}" — ${imageUrls.length} image(s)`
      );

      // try {
      //   const uploadedUrls = await Promise.all(
      //     imageUrls.map((url, idx) => limitImage(() => processOneImage(url, productTitle, idx)))
      //   );
      //   row[imagesCol] = uploadedUrls.join(",");
      //   successRows.push(row);
      // } catch (err) {
      //   console.error(`❌ Row ${i + 1} failed: ${err.message}`);
      //   row[imagesCol] = "";
      //   failedRows.push({ ...row, __error: err.message });
      // }

      const uploadedUrls = [];

      for (const [idx, url] of imageUrls.entries()) {
        try {
          const result = await limitImage(() => processOneImage(url, productTitle, idx));
          uploadedUrls.push(result);
        } catch (err) {
          console.warn(`⚠️ Image ${idx + 1} failed for "${productTitle}": ${err.message}`);
        }
      }

      if (uploadedUrls.length > 0) {
        row[imagesCol] = uploadedUrls.join(", ");
        successRows.push(row);
      } else {
        console.error(`❌ All images failed for "${productTitle}"`);
        row[imagesCol] = "";
        failedRows.push({ ...row, __error: "All images failed" });
      }

      processedCount++;
      await exportChunkIfNeeded();
    })
  );

  await Promise.all(tasks);
  await exportChunkIfNeeded(true);
  console.log(`✅ Finished ${fileName}`);
}

// ====== MAIN ======
async function run() {
  const csvFiles = fs.readdirSync(INPUT_DIR).filter((f) => f.toLowerCase().endsWith(".csv"));

  if (!csvFiles.length) {
    console.error("❌ No CSV files found in inputs/ folder.");
    process.exit(1);
  }

  for (const file of csvFiles) {
    const fullPath = path.join(INPUT_DIR, file);
    try {
      await processCsvFile(fullPath);
    } catch (e) {
      console.error(`❌ Failed file ${file}:`, e.message);
    }
  }

  await exiftool.end();
}

run().catch(async (e) => {
  console.error(e);
  try {
    await exiftool.end();
  } catch { }
  process.exit(1);
});
