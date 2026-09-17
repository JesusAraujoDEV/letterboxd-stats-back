const { fetchMovieDetailsByTitleYear } = require("../utils/tmdbHelper");
const { buildTmdbCacheKey } = require("../utils/cacheKey");

const buildTotalHoursWatched = async (diaryRows, detailsCache) => {
  let totalMinutes = 0;

  for (const row of diaryRows) {
    const title = row.Name || row.Title;
    if (!title) continue;

    const yearKey = Number.isFinite(Number(row.Year)) ? String(Number(row.Year)) : "";
    const cacheKey = buildTmdbCacheKey(title, yearKey);

    let details;
    if (detailsCache && Object.prototype.hasOwnProperty.call(detailsCache, cacheKey)) {
      details = detailsCache[cacheKey];
    } else {
      details = await fetchMovieDetailsByTitleYear(title, yearKey || null);
      if (detailsCache) detailsCache[cacheKey] = details;
    }

    const runtime = details && Number.isFinite(details.runtime) ? details.runtime : 0;
    if (runtime > 0) totalMinutes += runtime;
  }

  return Math.round(totalMinutes / 60);
};

module.exports = { buildTotalHoursWatched };
