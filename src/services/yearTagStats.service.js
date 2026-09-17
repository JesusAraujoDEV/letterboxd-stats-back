const { toTopN } = require("../utils/csvHelper");

const buildYearStats = (watchedRows) => {
  const yearCounter = {};
  watchedRows.forEach((row) => {
    const year = row.Year || row["Year Released"] || row["Release Year"];
    const key = year ? String(year).trim() : "";
    if (key) yearCounter[key] = (yearCounter[key] || 0) + 1;
  });

  const topYears = toTopN(yearCounter, 5, "year");
  const years = Object.keys(yearCounter).map(Number).filter(Number.isFinite);
  const moviesByReleaseYear = [];

  if (years.length > 0) {
    const minYear = Math.min(...years);
    const maxYear = Math.max(...years);
    for (let year = minYear; year <= maxYear; year += 1) {
      moviesByReleaseYear.push({ year: String(year), count: yearCounter[String(year)] || 0 });
    }
  }

  return { topYears, moviesByReleaseYear };
};

const buildTagStats = (diaryRows) => {
  const tagCounter = {};
  diaryRows.forEach((row) => {
    if (!row.Tags) return;
    String(row.Tags)
      .split(",")
      .map((tag) => tag.trim())
      .filter(Boolean)
      .forEach((tag) => (tagCounter[tag] = (tagCounter[tag] || 0) + 1));
  });

  return toTopN(tagCounter, 5, "tag");
};

const buildYearTagStats = (watchedRows, diaryRows) => ({
  ...buildYearStats(watchedRows),
  topTags: buildTagStats(diaryRows),
});

module.exports = { buildYearTagStats };
