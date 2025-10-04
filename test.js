/**
 * download_image.js
 * -----------------
 * Tải ảnh từ URL về thư mục gốc (cùng nơi với file script này)
 * - Giữ nguyên chất lượng gốc (không nén, không resize)
 * - Tự động đặt tên file theo tên ảnh trong URL
 */

import fs from "fs";
import path from "path";
import fetch from "node-fetch";

// URL ảnh cần tải (có thể thay đổi hoặc nhận từ process.argv)
const url = process.argv[2] || "https://image.spreadshirtmedia.com/image-server/v1/products/T812A2PA4267PT17X25Y60D1050491162W29500H11908/views/1,width=800,height=800,appearanceId=2,backgroundColor=F2F2F2,modelId=5669,crop=detail/acdc-classic-logo-with-lightning-bolt-mens-premium-t-shirt.jpg";

async function downloadImage(url) {
  try {
    console.log(`🔗 Downloading: ${url}`);

    const res = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": url,
      },
    });

    if (!res.ok) throw new Error(`HTTP Error: ${res.status} ${res.statusText}`);

    const arrayBuffer = await res.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    // Lấy tên file từ URL
    const fileName = path.basename(new URL(url).pathname);
    const outputPath = path.resolve(`./${fileName}`);

    fs.writeFileSync(outputPath, buffer);
    console.log(`✅ Saved: ${outputPath}`);
  } catch (err) {
    console.error("❌ Download failed:", err.message);
  }
}

downloadImage(url);
