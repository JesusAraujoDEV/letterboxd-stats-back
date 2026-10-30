const filmKey = (row) => `${row.Name}|${row.Year}`;

const addFilms = (films, rows, flags) => {
  rows
    .filter((row) => row["Letterboxd URI"])
    .forEach((row) => {
      const key = filmKey(row);
      const film = films.get(key) || {
        key,
        uri: row["Letterboxd URI"],
        title: row.Name,
        year: Number(row.Year) || null,
        rating: null,
      };
      films.set(key, { ...film, ...flags, rating: Number(row.Rating) || film.rating });
    });
};

const addEntries = (entries, rows) => {
  rows
    .filter((row) => row["Watched Date"])
    .forEach((row) => {
      const id = `${filmKey(row)}|${row["Watched Date"]}`;
      const entry = entries.get(id) || {};
      entries.set(id, {
        filmKey: filmKey(row),
        watchedOn: row["Watched Date"],
        rating: Number(row.Rating) || entry.rating || null,
        rewatch: row.Rewatch === "Yes" || Boolean(entry.rewatch),
        tags: row.Tags || entry.tags || null,
        review: row.Review || entry.review || null,
      });
    });
};

// Consolida los CSV por película. Las filas de diary/reviews traen la URI de la entrada, no la del film.
const buildSnapshot = (csvs) => {
  const films = new Map();
  addFilms(films, csvs.watchedRows, { watched: true });
  addFilms(films, csvs.ratingsRows, { watched: true });
  addFilms(films, csvs.likedFilmsRows, { liked: true });
  addFilms(films, csvs.watchlistRows, { inWatchlist: true });

  const entries = new Map();
  addEntries(entries, csvs.diaryRows);
  addEntries(entries, csvs.reviewsRows);

  const diary = [...entries.values()];
  const cutoff = diary.reduce((max, entry) => (entry.watchedOn > max ? entry.watchedOn : max), "");
  return { films: [...films.values()], diary, cutoff: cutoff || null };
};

module.exports = { buildSnapshot };
