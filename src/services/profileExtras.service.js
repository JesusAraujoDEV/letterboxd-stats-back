// Favoritos declarados en profile.csv y listas personalizadas (lists/*.csv) — ninguno
// de los dos se procesaba antes.

const { parseListCsvBuffer } = require("../utils/csvHelper");
const { fetchMovieDetailsByTitleYear } = require("../utils/tmdbHelper");
const { mapWithConcurrency } = require("../utils/concurrency");

const FILM_SLUG_PATTERN = /\/film\/([^/]+)\//;

const parseFavoriteFilmLinks = (profileRow) =>
  String(profileRow["Favorite Films"] || "")
    .split(",")
    .map((link) => link.trim())
    .filter(Boolean);

const slugToTitle = (slug) =>
  slug
    .split("-")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");

const resolveFavoriteFilmLink = async (shortUrl) => {
  try {
    const response = await fetch(shortUrl, {
      method: "HEAD",
      redirect: "follow",
      signal: AbortSignal.timeout(4000),
    });

    const match = response.url.match(FILM_SLUG_PATTERN);
    if (!match) return null;

    const title = slugToTitle(match[1]);
    const details = await fetchMovieDetailsByTitleYear(title, null);
    return { title, posterPath: details ? details.poster_path : null };
  } catch {
    return null;
  }
};

const buildFavoriteFilms = async (profileRow) => {
  const links = parseFavoriteFilmLinks(profileRow);
  const resolved = await mapWithConcurrency(links, 4, resolveFavoriteFilmLink);
  return resolved.filter(Boolean);
};

const buildCustomLists = async (zip) => {
  const listEntries = zip.getEntries().filter((entry) => /^lists\/.+\.csv$/i.test(entry.entryName));

  const lists = await Promise.all(
    listEntries.map(async (entry) => {
      const parsed = await parseListCsvBuffer(entry.getData());
      return { name: parsed.name, description: parsed.description, filmCount: parsed.films.length };
    }),
  );

  return lists;
};

module.exports = {
  buildFavoriteFilms,
  buildCustomLists,
};
