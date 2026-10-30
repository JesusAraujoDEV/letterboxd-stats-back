const { listFilms, listWatchlist, listRecommendations } = require("../services/libraryRead.service");
const { getHealth } = require("../services/syncRun.service");
const { importZip } = require("../services/zipImport.service");

const sendJson = (load) => async (req, res) => {
  try {
    return res.json(await load(req));
  } catch (err) {
    console.error("Error en la biblioteca:", err.message);
    return res.status(500).json({ error: "Error interno del servidor." });
  }
};

const importLibrary = async (req, res) => {
  if (!req.file || !req.file.buffer) {
    return res.status(400).json({ error: "Archivo requerido en el campo 'file'." });
  }

  try {
    return res.json(await importZip(req.file.buffer));
  } catch (err) {
    const message = err && err.message ? err.message : "Error interno del servidor.";
    const status = message.includes("ZIP") || message.includes("no encontrado") ? 400 : 500;
    return res.status(status).json({ error: message });
  }
};

module.exports = {
  getFilms: sendJson(listFilms),
  getWatchlist: sendJson(listWatchlist),
  getRecommendations: sendJson(listRecommendations),
  getLibraryHealth: sendJson(getHealth),
  importLibrary,
};
