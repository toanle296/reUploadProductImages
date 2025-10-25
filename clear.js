const fs = require("fs");
const path = require("path");

const folderPath = path.join(__dirname, "processed_imgs");

if (!fs.existsSync(folderPath)) {
  console.error("❌ Thư mục processed_imgs không tồn tại!");
  process.exit(1);
}

try {
  const files = fs.readdirSync(folderPath);
  let count = 0;

  for (const file of files) {
    const filePath = path.join(folderPath, file);
    const stat = fs.statSync(filePath);

    if (stat.isFile()) {
      fs.unlinkSync(filePath);
      count++;
    } else if (stat.isDirectory()) {
      // Xoá cả thư mục con nếu có
      fs.rmSync(filePath, { recursive: true, force: true });
      count++;
    }
  }

  console.log(`✅ Đã xóa ${count} mục trong thư mục processed_imgs`);
} catch (err) {
  console.error("❌ Lỗi khi xóa file:", err.message);
}
