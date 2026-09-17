// Estadísticas derivadas de columnas de diary.csv/reviews.csv/ratings.csv que ya se
// parsean pero no se usaban (Rewatch explícito, texto de reseñas, extremos de rating y runtime).
const { buildTmdbCacheKey } = require("../utils/cacheKey");

const isRewatchRow = (row) => String(row.Rewatch || "").trim().toLowerCase() === "yes";

const average = (values) =>
  values.length ? Number((values.reduce((sum, v) => sum + v, 0) / values.length).toFixed(2)) : null;

const buildRewatchStats = (diaryRows) => {
  const firstWatchRatings = [];
  const rewatchRatings = [];
  let rewatchCount = 0;

  diaryRows.forEach((row) => {
    const isRewatch = isRewatchRow(row);
    if (isRewatch) rewatchCount += 1;

    const rating = parseFloat(row.Rating);
    if (!Number.isFinite(rating)) return;
    (isRewatch ? rewatchRatings : firstWatchRatings).push(rating);
  });

  return {
    totalRewatches: rewatchCount,
    rewatchPercentage: diaryRows.length
      ? Number(((rewatchCount / diaryRows.length) * 100).toFixed(1))
      : 0,
    averageRatingFirstWatch: average(firstWatchRatings),
    averageRatingRewatch: average(rewatchRatings),
  };
};

const buildReviewTextStats = (reviewsRows) => {
  let totalWords = 0;
  let longest = null;

  reviewsRows.forEach((row) => {
    const text = row.Review || "";
    if (!text) return;

    const wordCount = text.trim().split(/\s+/).filter(Boolean).length;
    totalWords += wordCount;
    if (!longest || wordCount > longest.wordCount) {
      longest = { title: row.Name || "", year: row.Year || "", wordCount };
    }
  });

  return { totalWordsWritten: totalWords, longestReview: longest };
};

const buildRatingExtremes = (ratingsRows, detailsCache, limit = 5) => {
  const rated = ratingsRows
    .map((row) => ({
      title: row.Name ? String(row.Name).trim() : "",
      year: row.Year || "",
      rating: parseFloat(row.Rating),
    }))
    .filter((movie) => movie.title && Number.isFinite(movie.rating))
    .map((movie) => {
      const details = detailsCache[buildTmdbCacheKey(movie.title, movie.year)];
      return { ...movie, posterPath: details ? details.poster_path : null };
    });

  const sorted = [...rated].sort((a, b) => b.rating - a.rating);
  return {
    highest: sorted.slice(0, limit),
    lowest: sorted.slice(-limit).reverse(),
  };
};

const buildRuntimeExtremes = (diaryRows, detailsCache) => {
  const seen = new Map();

  diaryRows.forEach((row) => {
    const title = row.Name ? String(row.Name).trim() : "";
    if (!title) return;

    const key = buildTmdbCacheKey(title, row.Year);
    if (seen.has(key)) return;

    const details = detailsCache[key];
    const runtime = details && Number.isFinite(details.runtime) ? details.runtime : 0;
    if (runtime > 0) {
      seen.set(key, { title, year: row.Year || "", runtime, posterPath: details.poster_path || null });
    }
  });

  const movies = Array.from(seen.values()).sort((a, b) => b.runtime - a.runtime);
  if (movies.length === 0) return null;

  return { longest: movies[0], shortest: movies[movies.length - 1] };
};

const buildWatchSpan = (diaryRows) => {
  const withDates = diaryRows
    .map((row) => ({
      title: row.Name ? String(row.Name).trim() : "",
      date: row["Watched Date"] || row.Date || "",
    }))
    .filter((entry) => entry.title && entry.date)
    .sort((a, b) => a.date.localeCompare(b.date));

  if (withDates.length === 0) return null;
  return { first: withDates[0], last: withDates[withDates.length - 1] };
};

module.exports = {
  buildRewatchStats,
  buildReviewTextStats,
  buildRatingExtremes,
  buildRuntimeExtremes,
  buildWatchSpan,
};
