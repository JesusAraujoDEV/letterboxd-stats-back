const crypto = require("crypto");

const sha256 = (value) => crypto.createHash("sha256").update(value).digest();

const requireToken = (req, res, next) => {
  const expected = process.env.LIBRARY_API_TOKEN;
  if (!expected) {
    return res.status(503).json({ error: "LIBRARY_API_TOKEN no configurado." });
  }

  const provided = (req.headers.authorization || "").replace(/^Bearer /, "");
  if (!crypto.timingSafeEqual(sha256(provided), sha256(expected))) {
    return res.status(401).json({ error: "Token inválido." });
  }
  return next();
};

module.exports = { requireToken };
