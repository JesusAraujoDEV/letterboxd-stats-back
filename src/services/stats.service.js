const AdmZip = require("adm-zip");
const { parseCsvBuffer, getZipEntryBuffer, toTopN } = require("../utils/csvHelper");
const { fetchMoviePosterPath, fetchMovieDetailsByTitleYear } = require("../utils/tmdbHelper");

const buildTopDecades = async (ratingsRows) => {
  const decadeMap = {};

  ratingsRows.forEach((row) => {
    const title = row.Name || row.name || row.Title || row["Name"] || row["Title"];
    const yearValue = row.Year || row.year || row["Year"];
    const ratingValue = row.Rating || row.rating || row["Rating"];
    const ratedDate = row.Date || row.date || row["Date"] || null;

    const year = Number(yearValue);
    const rating = parseFloat(ratingValue);

    if (!title || !Number.isFinite(year) || !Number.isFinite(rating)) {
      return;
    }

    const decade = Math.floor(year / 10) * 10;
    const key = String(decade);

    if (!decadeMap[key]) {
      decadeMap[key] = { sum: 0, count: 0, movies: [] };
    }

    decadeMap[key].sum += rating;
    decadeMap[key].count += 1;
    decadeMap[key].movies.push({
      title: String(title).trim(),
      year: String(year),
      userRating: rating,
      ratedDate: ratedDate ? String(ratedDate).trim() : null,
    });
  });

  const decadeAverages = Object.entries(decadeMap)
    .map(([decade, data]) => ({
      decade: Number(decade),
      average: data.count > 0 ? Number((data.sum / data.count).toFixed(2)) : 0,
      movies: data.movies,
    }))
    .sort((a, b) => b.average - a.average)
    .slice(0, 3);

  const sortMovies = (a, b) => {
    if (b.userRating !== a.userRating) return b.userRating - a.userRating;
    if (a.ratedDate && b.ratedDate && a.ratedDate !== b.ratedDate) {
      return b.ratedDate.localeCompare(a.ratedDate);
    }
    return (a.title || "").localeCompare(b.title || "");
  };

  const enrichWithPosterPaths = async (movies) => {
    const batchSize = 5;
    const enriched = movies.map((movie) => ({ ...movie, posterPath: null }));

    for (let i = 0; i < enriched.length; i += batchSize) {
      const batch = enriched.slice(i, i + batchSize);
      const posterPaths = await Promise.all(
        batch.map((movie) => fetchMoviePosterPath(movie.title, movie.year)),
      );

      posterPaths.forEach((posterPath, index) => {
        batch[index].posterPath = posterPath || null;
      });
    }

    return enriched;
  };

  const topDecades = [];
  for (const entry of decadeAverages) {
    const topMovies = entry.movies.sort(sortMovies).slice(0, 8);
    const topMoviesWithPosters = await enrichWithPosterPaths(topMovies);
    topDecades.push({
      decade: `${entry.decade}s`,
      average: entry.average,
      topMovies: topMoviesWithPosters,
    });
  }

  return topDecades;
};

const buildTopMetadataFromWatched = async (watchedRows) => {
  const uniqueMovies = new Map();

  watchedRows.forEach((row) => {
    const title = row.Name || row.name || row.Title || row["Name"] || row["Title"];
    if (!title) return;

    const yearValue = row.Year || row.year || row["Year"] || row["Year Released"] || row["Release Year"];
    const year = Number(yearValue);
    const normalizedYear = Number.isFinite(year) ? String(year) : "";
    const key = `${String(title).trim().toLowerCase()}::${normalizedYear}`;

    if (!uniqueMovies.has(key)) {
      uniqueMovies.set(key, { title: String(title).trim(), year: normalizedYear || null });
    }
  });

  const movies = Array.from(uniqueMovies.values());
  const genreCounter = {};
  const countryCounter = {};
  const languageCounter = {};
  const batchSize = 25;
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  for (let i = 0; i < movies.length; i += batchSize) {
    const batch = movies.slice(i, i + batchSize);
    const detailsList = await Promise.all(
      batch.map((movie) => fetchMovieDetailsByTitleYear(movie.title, movie.year)),
    );

    detailsList.forEach((details) => {
      if (!details) return;

      const genres = Array.isArray(details.genres) ? details.genres : [];
      const countries = Array.isArray(details.production_countries)
        ? details.production_countries
        : [];
      const languages = Array.isArray(details.spoken_languages) ? details.spoken_languages : [];

      genres.forEach((genre) => {
        const name = genre && genre.name ? String(genre.name).trim() : "";
        if (name) genreCounter[name] = (genreCounter[name] || 0) + 1;
      });

      const primaryCountry = countries[0];
      const primaryCountryName = primaryCountry && (primaryCountry.name || primaryCountry.iso_3166_1);
      if (primaryCountryName) {
        const name = String(primaryCountryName).trim();
        if (name) countryCounter[name] = (countryCounter[name] || 0) + 1;
      }

      const primaryLanguage = languages[0];
      const primaryLanguageName = primaryLanguage && (primaryLanguage.english_name || primaryLanguage.name);
      if (primaryLanguageName) {
        const name = String(primaryLanguageName).trim();
        if (name) languageCounter[name] = (languageCounter[name] || 0) + 1;
      }
    });

    if (i + batchSize < movies.length) {
      await delay(200);
    }
  }

  const allCountries = Object.entries(countryCounter)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ name, count }));

  return {
    topGenres: toTopN(genreCounter, 10, "name"),
    topCountries: toTopN(countryCounter, 10, "name"),
    topLanguages: toTopN(languageCounter, 10, "name"),
    allCountries,
  };
};

const buildStatsFromZipBuffer = async (zipBuffer) => {
  let zip;
  try {
    zip = new AdmZip(zipBuffer);
  } catch (err) {
    throw new Error("El archivo no es un ZIP válido.");
  }

  const watchedBuffer = getZipEntryBuffer(zip, "watched.csv");
  const ratingsBuffer = getZipEntryBuffer(zip, "ratings.csv");
  const diaryBuffer = getZipEntryBuffer(zip, "diary.csv");

  const safeParseCsv = async (filename) => {
    try {
      const buffer = getZipEntryBuffer(zip, filename);
      return await parseCsvBuffer(buffer);
    } catch (err) {
      return [];
    }
  };

  const [
    watchedRows,
    ratingsRows,
    diaryRows,
    profileRows,
    watchlistRows,
    reviewsRows,
    commentsRows,
    deletedDiaryRows,
    deletedReviewsRows,
    deletedCommentsRows,
    likedFilmsRows,
    likedListsRows,
    likedReviewsRows,
  ] = await Promise.all([
    parseCsvBuffer(watchedBuffer),
    parseCsvBuffer(ratingsBuffer),
    parseCsvBuffer(diaryBuffer),
    safeParseCsv("profile.csv"),
    safeParseCsv("watchlist.csv"),
    safeParseCsv("reviews.csv"),
    safeParseCsv("comments.csv"),
    safeParseCsv("deleted/diary.csv"),
    safeParseCsv("deleted/reviews.csv"),
    safeParseCsv("deleted/comments.csv"),
    safeParseCsv("likes/films.csv"),
    safeParseCsv("likes/lists.csv"),
    safeParseCsv("likes/reviews.csv"),
  ]);

  const profileRow = profileRows[0] || {};
  const profile = {
    username: profileRow.Username || profileRow.username || "",
    location: profileRow.Location || profileRow.location || "",
    bio: profileRow.Bio || profileRow.bio || "",
  };

  const totalMovies = watchedRows.length;
  const totalLoggedMovies = diaryRows.length;

  let ratingSum = 0;
  let ratingCount = 0;
  const ratingDistribution = {};

  ratingsRows.forEach((row) => {
    const raw = row.Rating || row.rating || row["Rating"];
    const rating = parseFloat(raw);
    if (Number.isFinite(rating)) {
      ratingSum += rating;
      ratingCount += 1;
      const key = rating.toString();
      ratingDistribution[key] = (ratingDistribution[key] || 0) + 1;
    }
  });

  const averageRating = ratingCount > 0 ? Number((ratingSum / ratingCount).toFixed(2)) : 0;

  const ratingYearMap = {};
  ratingsRows.forEach((row) => {
    const yearValue = row.Year || row.year || row["Year"];
    const ratingValue = row.Rating || row.rating || row["Rating"];
    const year = Number(yearValue);
    const rating = parseFloat(ratingValue);

    if (Number.isFinite(year) && Number.isFinite(rating)) {
      const key = String(year);
      if (!ratingYearMap[key]) {
        ratingYearMap[key] = { sum: 0, count: 0 };
      }
      ratingYearMap[key].sum += rating;
      ratingYearMap[key].count += 1;
    }
  });

  const ratingYearKeys = Object.keys(ratingYearMap)
    .map((year) => Number(year))
    .filter((year) => Number.isFinite(year));
  const minRatingYear = ratingYearKeys.length > 0 ? Math.min(...ratingYearKeys) : 0;
  const maxRatingYear = ratingYearKeys.length > 0 ? Math.max(...ratingYearKeys) : 0;
  const averageRatingByReleaseYear = [];

  if (minRatingYear && maxRatingYear) {
    for (let year = minRatingYear; year <= maxRatingYear; year += 1) {
      const entry = ratingYearMap[String(year)];
      const average = entry && entry.count > 0 ? Number((entry.sum / entry.count).toFixed(2)) : 0;
      averageRatingByReleaseYear.push({ year: String(year), average });
    }
  }

  const yearCounter = {};
  watchedRows.forEach((row) => {
    const year = row.Year || row["Year"] || row["Year Released"] || row["Release Year"];
    if (year) {
      const key = String(year).trim();
      if (key) {
        yearCounter[key] = (yearCounter[key] || 0) + 1;
      }
    }
  });

  const topYears = toTopN(yearCounter, 5, "year");
  const yearKeys = Object.keys(yearCounter)
    .map((year) => Number(year))
    .filter((year) => Number.isFinite(year));
  const minYear = yearKeys.length > 0 ? Math.min(...yearKeys) : 0;
  const maxYear = yearKeys.length > 0 ? Math.max(...yearKeys) : 0;
  const moviesByReleaseYear = [];

  if (minYear && maxYear) {
    for (let year = minYear; year <= maxYear; year += 1) {
      const count = yearCounter[String(year)] || 0;
      moviesByReleaseYear.push({ year: String(year), count });
    }
  }

  const tagCounter = {};
  diaryRows.forEach((row) => {
    const tagsValue = row.Tags || row.tags || row["Tags"];
    if (!tagsValue) return;

    const tags = String(tagsValue)
      .split(",")
      .map((tag) => tag.trim())
      .filter(Boolean);

    tags.forEach((tag) => {
      tagCounter[tag] = (tagCounter[tag] || 0) + 1;
    });
  });

  const topTags = toTopN(tagCounter, 5, "tag");

  const totalWatchlist = watchlistRows.length;
  const totalReviews = reviewsRows.length;
  const totalComments = commentsRows.length;

  const deletedDiaryCount = deletedDiaryRows.length;
  const deletedReviewsCount = deletedReviewsRows.length;
  const deletedCommentsCount = deletedCommentsRows.length;

  const deletedListsEntries = zip
    .getEntries()
    .filter(
      (entry) =>
        entry.entryName.toLowerCase().startsWith("deleted/lists/") &&
        entry.entryName.toLowerCase().endsWith(".csv"),
    );
  const deletedListsCount = deletedListsEntries.length;
  const deletedListsNames = deletedListsEntries.map((entry) => {
    const baseName = entry.entryName.split("/").pop() || "";
    return baseName.replace(/\.csv$/i, "").replace(/-/g, " ").trim();
  });

  const totalLikedFilms = likedFilmsRows.length;
  const totalLikedLists = likedListsRows.length;
  const totalLikedReviews = likedReviewsRows.length;

  const likedYearCounter = {};
  likedFilmsRows.forEach((row) => {
    const year = row.Year || row.year || row["Year"];
    if (year) {
      const key = String(year).trim();
      if (key) {
        likedYearCounter[key] = (likedYearCounter[key] || 0) + 1;
      }
    }
  });
  const topLikedYears = toTopN(likedYearCounter, 3, "year");

  const topDecades = await buildTopDecades(ratingsRows);
  const { topGenres, topCountries, topLanguages, allCountries } = await buildTopMetadataFromWatched(
    watchedRows,
  );

  return {
    profile,
    totalMovies,
    totalLoggedMovies,
    averageRating,
    ratingDistribution,
    topYears,
    moviesByReleaseYear,
    averageRatingByReleaseYear,
    topTags,
    totalWatchlist,
    totalReviews,
    totalComments,
    deletedDiaryCount,
    deletedReviewsCount,
    deletedCommentsCount,
    deletedListsCount,
    deletedListsNames,
    totalLikedFilms,
    totalLikedLists,
    totalLikedReviews,
    topLikedYears,
    topDecades,
    topGenres,
    topCountries,
    topLanguages,
    allCountries,
  };
};

module.exports = {
  buildStatsFromZipBuffer,
};
