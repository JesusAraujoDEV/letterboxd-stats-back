const { query } = require("../utils/db");

const POSTER_BASE_URL = "https://image.tmdb.org/t/p/w500";

const toPosterUrl = (path) => (path ? `${POSTER_BASE_URL}${path}` : null);

const toFilm = (row) => ({
  tmdbId: row.tmdb_id,
  title: row.title,
  year: row.year,
  rating: row.rating,
  watchedOn: row.watched_on,
  posterUrl: toPosterUrl(row.poster_path),
});

const listFilmsWhere = async (condition) => {
  const { rows } = await query(
    `SELECT f.tmdb_id, f.title, f.year, f.rating::float8 AS rating, f.poster_path,
       (SELECT max(d.watched_on)::text FROM letterboxd.diary_entries d WHERE d.slug = f.slug) AS watched_on
     FROM letterboxd.films f
     WHERE ${condition}
     ORDER BY watched_on DESC NULLS LAST, f.title`,
  );
  return rows.map(toFilm);
};

const listFilms = () => listFilmsWhere("f.watched");

const listWatchlist = () => listFilmsWhere("f.in_watchlist");

const listRecommendations = async () => {
  const { rows } = await query(
    `SELECT tmdb_id, title, year, poster_path, reason, netflix_countries, netflix_id
     FROM letterboxd.recommendations ORDER BY score DESC, title`,
  );
  return rows.map((row) => ({
    tmdbId: row.tmdb_id,
    title: row.title,
    year: row.year,
    posterUrl: toPosterUrl(row.poster_path),
    reason: row.reason,
    netflixCountries: row.netflix_countries,
    netflixId: row.netflix_id,
  }));
};

module.exports = { listFilms, listWatchlist, listRecommendations };
