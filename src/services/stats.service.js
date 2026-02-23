const AdmZip = require("adm-zip");
const { parseCsvBuffer, getZipEntryBuffer, toTopN } = require("../utils/csvHelper");

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
  ] = await Promise.all([
    parseCsvBuffer(watchedBuffer),
    parseCsvBuffer(ratingsBuffer),
    parseCsvBuffer(diaryBuffer),
    safeParseCsv("profile.csv"),
    safeParseCsv("watchlist.csv"),
    safeParseCsv("reviews.csv"),
    safeParseCsv("comments.csv"),
  ]);

  const profileRow = profileRows[0] || {};
  const profile = {
    username: profileRow.Username || profileRow.username || "",
    location: profileRow.Location || profileRow.location || "",
    bio: profileRow.Bio || profileRow.bio || "",
  };

  const totalMovies = watchedRows.length;

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

  return {
    profile,
    totalMovies,
    averageRating,
    ratingDistribution,
    topYears,
    moviesByReleaseYear,
    topTags,
    totalWatchlist,
    totalReviews,
    totalComments,
  };
};

module.exports = {
  buildStatsFromZipBuffer,
};
