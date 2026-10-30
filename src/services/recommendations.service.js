const { query, withTransaction } = require("../utils/db");
const { mapWithConcurrency } = require("../utils/concurrency");
const { fetchMovieRecommendations } = require("../utils/tmdbHelper");
const { fetchNetflixAvailability } = require("./netflixAvailability.service");
const { rankCandidates } = require("./recommendationRanking.service");

const MAX_SEEDS = 20;
const MAX_RECOMMENDATIONS = 30;

const loadSeeds = async () => {
  const { rows } = await query(
    `SELECT tmdb_id, title, rating::float8 AS rating FROM letterboxd.films f
     WHERE watched AND rating >= 4 AND tmdb_id IS NOT NULL AND tmdb_type = 'movie'
     ORDER BY rating DESC,
       (SELECT max(watched_on) FROM letterboxd.diary_entries d WHERE d.slug = f.slug) DESC NULLS LAST
     LIMIT $1`,
    [MAX_SEEDS],
  );
  return rows;
};

const loadSeenIds = async () => {
  const { rows } = await query(
    "SELECT tmdb_id FROM letterboxd.films WHERE watched AND tmdb_id IS NOT NULL",
  );
  return new Set(rows.map((row) => row.tmdb_id));
};

const replaceRecommendations = (items) =>
  withTransaction(async (client) => {
    await client.query("DELETE FROM letterboxd.recommendations");
    for (const item of items) {
      await client.query(
        `INSERT INTO letterboxd.recommendations
           (tmdb_id, title, year, poster_path, reason, score, netflix_countries, netflix_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [
          item.tmdbId,
          item.title,
          item.year,
          item.posterPath,
          item.reason,
          item.score,
          item.netflixCountries,
          item.netflixId,
        ],
      );
    }
  });

const refreshRecommendations = async () => {
  const [seeds, seenIds] = await Promise.all([loadSeeds(), loadSeenIds()]);
  const seedResults = await mapWithConcurrency(seeds, 4, async (seed) => ({
    seed,
    results: await fetchMovieRecommendations(seed.tmdb_id),
  }));

  const candidates = await mapWithConcurrency(rankCandidates(seedResults, seenIds), 4, async (item) => ({
    ...item,
    ...(await fetchNetflixAvailability(item)),
  }));
  const items = candidates.filter((item) => item.netflixCountries.length).slice(0, MAX_RECOMMENDATIONS);
  if (!items.length) {
    throw new Error("Ninguna recomendación está en Netflix; se conservan las anteriores.");
  }

  await replaceRecommendations(items);
  return items.length;
};

module.exports = { refreshRecommendations };
