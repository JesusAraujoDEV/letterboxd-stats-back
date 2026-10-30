const { query } = require("../utils/db");
const { mapWithConcurrency } = require("../utils/concurrency");
const { fetchTmdbTitle } = require("../utils/tmdbHelper");

const upsertFilm = (client, film) =>
  client.query(
    `INSERT INTO letterboxd.films
       (slug, letterboxd_uri, tmdb_id, tmdb_type, title, year, rating, watched, liked, in_watchlist)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     ON CONFLICT (slug) DO UPDATE SET
       letterboxd_uri = COALESCE(EXCLUDED.letterboxd_uri, films.letterboxd_uri),
       tmdb_id = COALESCE(EXCLUDED.tmdb_id, films.tmdb_id),
       tmdb_type = EXCLUDED.tmdb_type,
       title = EXCLUDED.title,
       year = COALESCE(EXCLUDED.year, films.year),
       rating = COALESCE(EXCLUDED.rating, films.rating),
       watched = films.watched OR EXCLUDED.watched,
       liked = films.liked OR EXCLUDED.liked,
       in_watchlist = films.in_watchlist OR EXCLUDED.in_watchlist,
       updated_at = now()`,
    [
      film.slug,
      film.uri || null,
      film.tmdbId,
      film.tmdbType,
      film.title,
      film.year,
      film.rating,
      Boolean(film.watched),
      Boolean(film.liked),
      Boolean(film.inWatchlist),
    ],
  );

const upsertDiaryEntry = (client, entry) =>
  client.query(
    `INSERT INTO letterboxd.diary_entries (slug, watched_on, rating, rewatch, tags, review)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (slug, watched_on) DO UPDATE SET
       rating = COALESCE(EXCLUDED.rating, diary_entries.rating),
       rewatch = EXCLUDED.rewatch,
       tags = COALESCE(EXCLUDED.tags, diary_entries.tags),
       review = COALESCE(EXCLUDED.review, diary_entries.review)`,
    [entry.slug, entry.watchedOn, entry.rating, entry.rewatch, entry.tags || null, entry.review],
  );

const fillMissingPosters = async () => {
  const { rows } = await query(
    `SELECT slug, tmdb_id, tmdb_type FROM letterboxd.films
     WHERE tmdb_id IS NOT NULL AND poster_path IS NULL`,
  );

  await mapWithConcurrency(rows, 4, async (film) => {
    const details = await fetchTmdbTitle(film.tmdb_type, film.tmdb_id);
    if (!details || !details.poster_path) {
      return;
    }
    await query("UPDATE letterboxd.films SET poster_path = $2 WHERE slug = $1", [
      film.slug,
      details.poster_path,
    ]);
  });
};

module.exports = { upsertFilm, upsertDiaryEntry, fillMissingPosters };
