// ponytail: unifica la clave de caché de TMDB que antes vivía duplicada en 3 archivos
const buildTmdbCacheKey = (title, year) => {
  const safeTitle = title ? String(title).trim().toLowerCase() : "";
  const safeYear = year ? String(year).trim() : "";
  return `${safeTitle}::${safeYear}`;
};

module.exports = { buildTmdbCacheKey };
