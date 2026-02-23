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

  const [watchedRows, ratingsRows, diaryRows] = await Promise.all([
    parseCsvBuffer(watchedBuffer),
    parseCsvBuffer(ratingsBuffer),
    parseCsvBuffer(diaryBuffer),
  ]);

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

  return {
    totalMovies,
    averageRating,
    ratingDistribution,
    topYears,
    topTags,
  };
};

module.exports = {
  buildStatsFromZipBuffer,
};
