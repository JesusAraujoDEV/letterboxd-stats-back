const express = require("express");
const multer = require("multer");
const { uploadStats } = require("../controllers/stats.controller");

const router = express.Router();
// ponytail: 20MB cubre cualquier export real de Letterboxd (texto comprimido) con margen amplio
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

router.post("/upload-stats", (req, res, next) => {
  upload.single("file")(req, res, (err) => {
    if (err instanceof multer.MulterError && err.code === "LIMIT_FILE_SIZE") {
      return res.status(413).json({ error: "El archivo supera el límite de 20MB." });
    }
    if (err) return next(err);
    next();
  });
}, uploadStats);

module.exports = router;
