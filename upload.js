// upload.js
const AWS = require("aws-sdk");
const fs = require("fs");
const path = require("path");

const MAX_CONCURRENT_UPLOADS = 30; // giữ nguyên

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

	// Thêm AUTHOR_DEFAULT trước khi tạo key
	const nameWithoutExt = path.basename(fileName, fileExtension);
	const fileNameWithAuthor = `${nameWithoutExt}-${process.env.META_AUTHOR || "unknown"}${fileExtension}`;

	const key = s3Folder ? `${s3Folder.replace(/\/+$/,'')}/${fileNameWithAuthor}` : fileNameWithAuthor;
 

    await s3
      .upload({
        Bucket: bucketName,
        Key: key,
        Body: fileContent,
        ACL: "public-read",
        ContentType: contentType
      })
      .promise();

    // ✅ Trả URL có kèm folder (nếu có)
    return `${endpointUrl.replace(/\/+$/,'')}/${bucketName}/${key}`;
  } catch (error) {
    console.error(`❌ Error uploading ${filePath}:`, error);
    return null;
  }
}

// (giữ nguyên hàm uploadFolderToS3 nếu bạn còn dùng ở nơi khác)

module.exports = { uploadToS3, MAX_CONCURRENT_UPLOADS };
