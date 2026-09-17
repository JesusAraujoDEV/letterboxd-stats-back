const { toTopN } = require("../utils/csvHelper");
const { fetchMovieDetailsByTitleYear } = require("../utils/tmdbHelper");
const { buildTmdbCacheKey } = require("../utils/cacheKey");
const { parseWatchedDate } = require("../utils/dateHelpers");
const { languageCodeMap } = require("../utils/languageMap");
const { countryCodeMap } = require("../utils/countryMap");
const { extractMovieExtras } = require("./movieExtras.service");

const BATCH_SIZE = 25;
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const buildUniqueMovies = (watchedRows) => {
  const uniqueMovies = new Map();
  watchedRows.forEach((row) => {
    const title = row.Name || row.Title;
    if (!title) return;
    const year = Number(row.Year || row["Year Released"] || row["Release Year"]);
    const normalizedYear = Number.isFinite(year) ? String(year) : "";
    const key = buildTmdbCacheKey(title, normalizedYear);
    if (!uniqueMovies.has(key)) {
      uniqueMovies.set(key, { title: String(title).trim(), year: normalizedYear || null });
    }
  });
  return Array.from(uniqueMovies.values());
};

const buildDiaryByTitle = (diaryRows) => {
  const diaryByTitle = {};
  diaryRows.forEach((row) => {
    const title = row.Name || row.Title;
    const key = title ? String(title).trim() : "";
    if (!key) return;
    (diaryByTitle[key] = diaryByTitle[key] || []).push(row);
  });
  return diaryByTitle;
};

const fetchDetailsBatch = async (batch, detailsCache) =>
  Promise.all(
    batch.map(async (movie) => {
      const cacheKey = buildTmdbCacheKey(movie.title, movie.year);
      if (detailsCache && Object.prototype.hasOwnProperty.call(detailsCache, cacheKey)) {
        return detailsCache[cacheKey];
      }
      // ponytail: cachea también el null (caché negativa) — evita re-consultar TMDB en cada rewatch de una película no resoluble
      const details = await fetchMovieDetailsByTitleYear(movie.title, movie.year);
      if (detailsCache) detailsCache[cacheKey] = details;
      return details;
    }),
  );

const buildMovieObj = (movie, details, diaryByTitle, likedTitlesArray) => {
  const movieTitle = movie.title ? String(movie.title).trim() : "";
  const movieYearValue = movie.year ? Number(movie.year) : NaN;
  const diaryLogs = movieTitle ? diaryByTitle[movieTitle] || [] : [];

  const languageCode = details.original_language ? String(details.original_language).trim() : "";
  const originCountryCode = Array.isArray(details.origin_country)
    ? String(details.origin_country[0] || "").trim()
    : "";

  return {
    title: movieTitle,
    releaseYear: Number.isFinite(movieYearValue) ? movieYearValue : null,
    decade: Number.isFinite(movieYearValue) ? `${Math.floor(movieYearValue / 10) * 10}s` : null,
    posterPath: details.poster_path || null,
    liked: movieTitle ? likedTitlesArray.includes(movieTitle) : false,
    genres: Array.isArray(details.genres) ? details.genres.map((g) => g.name).filter(Boolean) : [],
    country: countryCodeMap[originCountryCode] || originCountryCode || null,
    language: languageCodeMap[languageCode] || languageCode || null,
    ...extractMovieExtras(details),
    directors: Array.isArray(details.credits && details.credits.crew)
      ? details.credits.crew.filter((c) => c && c.job === "Director" && c.name).map((d) => d.name)
      : [],
    cast: Array.isArray(details.credits && details.credits.cast)
      ? details.credits.cast.slice(0, 10).filter((c) => c && c.name).map((c) => c.name)
      : [],
    diaryLogs: diaryLogs.map((entry) => {
      const watchedMeta = parseWatchedDate(entry["Watched Date"]);
      return {
        rating: entry.Rating ? parseFloat(entry.Rating) : null,
        watchedDate: entry["Watched Date"],
        watchedYear: watchedMeta ? watchedMeta.year : null,
        watchedDay: watchedMeta ? watchedMeta.watchedDay : null,
        watchedWeek: watchedMeta ? watchedMeta.watchedWeek : null,
        watchedMonth: watchedMeta ? watchedMeta.watchedMonth : null,
        tags: entry.Tags ? String(entry.Tags).split(",").map((t) => t.trim()) : [],
      };
    }),
    rewatchCount: diaryLogs.length,
  };
};

const buildMovieMetadata = async (watchedRows, diaryRows, likedTitlesSet, detailsCache) => {
  const movies = buildUniqueMovies(watchedRows);
  const diaryByTitle = buildDiaryByTitle(diaryRows);
  const likedTitlesArray = Array.from(likedTitlesSet || []);

  const genreCounter = {};
  const countryCounter = {};
  const languageCounter = {};
  const allMovies = [];

  for (let i = 0; i < movies.length; i += BATCH_SIZE) {
    const batch = movies.slice(i, i + BATCH_SIZE);
    const detailsList = await fetchDetailsBatch(batch, detailsCache);

    detailsList.forEach((details, index) => {
      if (!details) return;
      const movieObj = buildMovieObj(batch[index], details, diaryByTitle, likedTitlesArray);
      allMovies.push(movieObj);

      movieObj.genres.forEach((name) => (genreCounter[name] = (genreCounter[name] || 0) + 1));
      if (movieObj.country) countryCounter[movieObj.country] = (countryCounter[movieObj.country] || 0) + 1;
      if (movieObj.language) languageCounter[movieObj.language] = (languageCounter[movieObj.language] || 0) + 1;
    });

    if (i + BATCH_SIZE < movies.length) await delay(200);
  }

  const allCountries = Object.entries(countryCounter)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ name, count }));

  return {
    topGenres: toTopN(genreCounter, 10, "name"),
    topCountries: toTopN(countryCounter, 10, "name"),
    topLanguages: toTopN(languageCounter, 10, "name"),
    allCountries,
    allMovies,
  };
};

module.exports = { buildMovieMetadata };
