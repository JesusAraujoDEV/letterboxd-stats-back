const express = require("express");
const multer = require("multer");
const { uploadStats } = require("../controllers/stats.controller");

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

router.post("/upload-stats", upload.single("file"), uploadStats);

module.exports = router;
