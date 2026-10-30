const express = require("express");
const {
  getFilms,
  getWatchlist,
  getRecommendations,
  getLibraryHealth,
  importLibrary,
} = require("../controllers/library.controller");
const { requireToken } = require("../utils/requireToken");
const { uploadZip } = require("../utils/uploadZip");

const router = express.Router();

router.get("/films", requireToken, getFilms);
router.get("/watchlist", requireToken, getWatchlist);
router.get("/recommendations", requireToken, getRecommendations);
router.get("/health", requireToken, getLibraryHealth);
router.post("/library/import", requireToken, uploadZip, importLibrary);

module.exports = router;
