const AdmZip = require("adm-zip");
const { query, withTransaction } = require("../utils/db");
const { mapWithConcurrency } = require("../utils/concurrency");
const { resolveFilmUri } = require("../utils/letterboxdHelper");
const { parseZipCsvEntries } = require("./zipInput.service");
const { buildSnapshot } = require("./zipSnapshot.service");
const { upsertFilm, upsertDiaryEntry, fillMissingPosters } = require("./libraryStore.service");
const { refreshRecommendations } = require("./recommendations.service");
const { recordRun } = require("./syncRun.service");

const loadKnownFilms = async () => {
  const { rows } = await query(
    `SELECT letterboxd_uri, slug, tmdb_id, tmdb_type FROM letterboxd.films
     WHERE letterboxd_uri IS NOT NULL`,
  );
  return new Map(
    rows.map((row) => [
      row.letterboxd_uri,
      { slug: row.slug, tmdbId: row.tmdb_id, tmdbType: row.tmdb_type },
    ]),
  );
};

const resolveFilms = async (films) => {
  const known = await loadKnownFilms();
  const resolved = await mapWithConcurrency(films, 4, async (film) => {
    const identity = known.get(film.uri) || (await resolveFilmUri(film.uri).catch(() => null));
    return identity && identity.slug ? { ...film, ...identity } : null;
  });
  return resolved.filter(Boolean);
};

const applySnapshot = (films, diary, cutoff) =>
  withTransaction(async (client) => {
    await client.query(
      `UPDATE letterboxd.films
       SET rating = NULL, watched = false, liked = false, in_watchlist = false`,
    );
    await client.query("DELETE FROM letterboxd.diary_entries WHERE watched_on <= $1", [cutoff]);

    const slugByKey = new Map(films.map((film) => [film.key, film.slug]));
    for (const film of films) {
      await upsertFilm(client, film);
    }
    for (const entry of diary.filter((item) => slugByKey.has(item.filmKey))) {
      await upsertDiaryEntry(client, { ...entry, slug: slugByKey.get(entry.filmKey) });
    }
    return diary.filter((item) => slugByKey.has(item.filmKey)).length;
  });

const importZip = (buffer) =>
  recordRun("zip", async () => {
    const snapshot = buildSnapshot(await parseZipCsvEntries(new AdmZip(buffer)));
    const films = await resolveFilms(snapshot.films);
    if (!films.length) {
      throw new Error("No se pudo resolver ninguna película del ZIP.");
    }

    const diaryEntries = await applySnapshot(films, snapshot.diary, snapshot.cutoff);
    await fillMissingPosters();
    return {
      films: films.length,
      filmsFailed: snapshot.films.length - films.length,
      diaryEntries,
      diarySkipped: snapshot.diary.length - diaryEntries,
      recommendations: await refreshRecommendations(),
    };
  });

module.exports = { importZip };
