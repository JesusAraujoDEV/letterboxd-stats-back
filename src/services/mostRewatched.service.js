const { fetchMoviePosterPath } = require("../utils/tmdbHelper");

// Nota: rewatch por duplicado de título en diary.csv — distinto de diaryExtras.service.js#buildRewatchStats,
// que usa la columna explícita `Rewatch` de Letterboxd. Son dos conceptos, no fusionar.
const buildMostRewatchedMovies = async (diaryRows) => {
  const rewatchCounts = {};

  diaryRows.forEach((row) => {
    const title = row.Name || row.Title;
    const key = title ? String(title).trim() : "";
    if (!key) return;
    if (!rewatchCounts[key]) rewatchCounts[key] = { title: key, count: 1 };
    else rewatchCounts[key].count += 1;
  });

  const base = Object.values(rewatchCounts)
    .filter((entry) => entry.count > 1)
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);

  return Promise.all(
    base.map(async (movie) => ({ ...movie, posterPath: await fetchMoviePosterPath(movie.title, null) })),
  );
};

module.exports = { buildMostRewatchedMovies };
