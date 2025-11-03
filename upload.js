// upload.js
const AWS = require("aws-sdk");
const fs = require("fs");
const path = require("path");
const SftpClient = require("ssh2-sftp-client");

const MAX_CONCURRENT_UPLOADS = 30;

// ====== Upload lên DO Spaces (giữ nguyên) ======
async function uploadToS3(filePath, bucketName, region, accessKey, secretKey, endpointUrl, s3Folder = "", author = "") {
  try {
    const s3 = new AWS.S3({
      region,
      accessKeyId: accessKey,
      secretAccessKey: secretKey,
      endpoint: endpointUrl,
      s3ForcePathStyle: true
    });

    const fileContent = fs.readFileSync(filePath);
    const fileName = path.basename(filePath);
    const fileExtension = path.extname(fileName).toLowerCase();

    const mimeTypes = {
      ".jpg": "image/jpeg",
      ".jpeg": "image/jpeg",
      ".png": "image/png",
      ".gif": "image/gif",
      ".webp": "image/webp"
    };
    const contentType = mimeTypes[fileExtension] || "application/octet-stream";

    let fileNameFinal;
    if (process.env.IMG_NAME_WITH_AUTHOR === "true") {
      const shortAuthor = (process.env.META_AUTHOR || "unk").substring(0, 3);
      fileNameFinal = `${path.basename(fileName, fileExtension)}-${shortAuthor}${fileExtension}`;
    } else {
      fileNameFinal = fileName;
    }

    const key = s3Folder ? `${s3Folder.replace(/\/+$/, '')}/${fileNameFinal}` : fileNameFinal;

    await s3.upload({
      Bucket: bucketName,
      Key: key,
      Body: fileContent,
      ACL: "public-read",
      ContentType: contentType
    }).promise();

    return `${endpointUrl.replace(/\/+$/, '')}/${bucketName}/${key}`;
  } catch (error) {
    console.error(`❌ Error uploading ${filePath}:`, error);
    return null;
  }
}

// ====== Upload lên hosting Cloudways qua SFTP ======
async function uploadToSFTP(filePath) {
  const sftp = new SftpClient();
  try {
    const host = process.env.SFTP_HOST;
    const port = parseInt(process.env.SFTP_PORT || "22", 10);
    const username = process.env.SFTP_USER;
    const password = process.env.SFTP_PASS;
    const remoteDir = process.env.SFTP_REMOTE_DIR || "/home/master/applications/yourapp/public_html/uploads/";

    if (!host || !username || !password) {
      throw new Error("Missing SFTP credentials in .env");
    }

    await sftp.connect({ host, port, username, password });

    // Tạo thư mục nếu chưa có
    try {
      await sftp.mkdir(remoteDir, true);
    } catch { /* ignore if exists */ }

    const fileName = path.basename(filePath);
    const remotePath = path.posix.join(remoteDir, fileName);
    // await sftp.fastPut(filePath, remotePath);
    const localPath = filePath.replace(/\\/g, "/"); // ✅ fix backslash to slash
    await sftp.fastPut(localPath, remotePath);


    // const baseUrl = process.env.SFTP_BASE_URL || "https://yourdomain.com/uploads";
    const baseUrl = process.env.SFTP_BASE_URL || `https://${process.env.META_AUTHOR}/wp-content/uploads`;

    const fileUrl = `${baseUrl.replace(/\/$/, "")}/${encodeURIComponent(fileName)}`;

    console.log(`✅ Uploaded via SFTP: ${fileUrl}`);
    await sftp.end();
    return fileUrl;
  } catch (err) {
    console.error(`❌ SFTP upload failed for ${filePath}:`, err.message);
    try { await sftp.end(); } catch { }
    return null;
  }
}

// ====== Wrapper chọn nơi upload ======
async function uploadAuto(filePath, ...args) {
  const mode = (process.env.UPLOAD_MODE || "spaces").toLowerCase();
  if (mode === "sftp") {
    return await uploadToSFTP(filePath);
  } else if (mode === "hosting") {
    console.error("⚠️ HOSTING mode (HTTP upload) is not configured in this version — use 'sftp' instead.");
    return null;
  }
  return await uploadToS3(filePath, ...args);
}

module.exports = { uploadToS3, uploadToSFTP, uploadAuto, MAX_CONCURRENT_UPLOADS };
