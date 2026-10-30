const express = require("express");
const { uploadStats } = require("../controllers/stats.controller");
const { uploadZip } = require("../utils/uploadZip");

const router = express.Router();

router.post("/upload-stats", uploadZip, uploadStats);

module.exports = router;
