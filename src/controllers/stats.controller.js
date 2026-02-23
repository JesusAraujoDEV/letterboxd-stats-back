const { buildStatsFromZipBuffer } = require("../services/stats.service");

const uploadStats = async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ error: "Archivo requerido en el campo 'file'." });
    }

    const stats = await buildStatsFromZipBuffer(req.file.buffer);
    return res.json(stats);
  } catch (err) {
    const message = err && err.message ? err.message : "Error interno del servidor.";
    const status = message.includes("ZIP") || message.includes("no encontrado") ? 400 : 500;
    return res.status(status).json({ error: message });
  }
};

module.exports = {
  uploadStats,
};
