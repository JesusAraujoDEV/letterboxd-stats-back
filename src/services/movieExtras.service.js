// Datos derivados de los campos de TMDB que ya se piden (collection, budget, revenue,
// vote_average, production_companies) pero no se usaban.
const { buildTmdbCacheKey } = require("../utils/cacheKey");

const extractMovieExtras = (details) => ({
  collection: details.belongs_to_collection ? details.belongs_to_collection.name : null,
  budget: Number.isFinite(details.budget) && details.budget > 0 ? details.budget : null,
  revenue: Number.isFinite(details.revenue) && details.revenue > 0 ? details.revenue : null,
  voteAverage: Number.isFinite(details.vote_average) ? details.vote_average : null,
  studios: Array.isArray(details.production_companies)
    ? details.production_companies.map((company) => company.name).filter(Boolean)
    : [],
});

const buildFranchiseStats = (allMovies, topN = 10) => {
  const counter = {};
  allMovies.forEach((movie) => {
    if (!movie.collection) return;
    counter[movie.collection] = (counter[movie.collection] || 0) + 1;
  });

  return Object.entries(counter)
    .filter(([, count]) => count > 1)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([name, count]) => ({ name, count }));
};

const buildStudioStats = (allMovies, topN = 10) => {
  const counter = {};
  allMovies.forEach((movie) => {
    (movie.studios || []).forEach((studio) => {
      counter[studio] = (counter[studio] || 0) + 1;
    });
  });

  return Object.entries(counter)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([name, count]) => ({ name, count }));
};

const buildIndustryTotals = (allMovies) => {
  let totalBudget = 0;
  let totalRevenue = 0;

  allMovies.forEach((movie) => {
    if (movie.budget) totalBudget += movie.budget;
    if (movie.revenue) totalRevenue += movie.revenue;
  });

  return { totalBudget, totalRevenue };
};

const buildRatingComparison = (ratingsRows, detailsCache) => {
  let yourSum = 0;
  let worldSum = 0;
  let count = 0;

  ratingsRows.forEach((row) => {
    const title = row.Name || row.Title;
    const rating = parseFloat(row.Rating);
    if (!title || !Number.isFinite(rating)) return;

    const details = detailsCache[buildTmdbCacheKey(title, row.Year)];
    if (!details || !Number.isFinite(details.vote_average)) return;

    yourSum += rating * 2; // TMDB usa escala 0-10, Letterboxd 0-5
    worldSum += details.vote_average;
    count += 1;
  });

  if (count === 0) return null;

  return {
    yourAverage: Number((yourSum / count / 2).toFixed(2)),
    worldAverage: Number((worldSum / count).toFixed(2)),
    sampleSize: count,
  };
};

module.exports = {
  extractMovieExtras,
  buildFranchiseStats,
  buildStudioStats,
  buildIndustryTotals,
  buildRatingComparison,
};
