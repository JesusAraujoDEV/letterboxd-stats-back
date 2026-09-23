// Desglose por año de visionado: cuántas fueron primera vez vs rewatch, con porcentajes.
const { isRewatchRow } = require("./diaryExtras.service");

const YEAR_PATTERN = /^[0-9]{4}$/;

const extractYear = (row) => {
  const raw = String(row["Watched Date"] || row.Date || "").slice(0, 4);
  return YEAR_PATTERN.test(raw) ? raw : null;
};

const toPercentage = (part, total) => Number(((part / total) * 100).toFixed(1));

const toYearStat = ([year, tally]) => {
  const { totalWatches, rewatches } = tally;
  const firstWatches = totalWatches - rewatches;
  return {
    year,
    totalWatches,
    firstWatches,
    rewatches,
    firstWatchPercentage: toPercentage(firstWatches, totalWatches),
    rewatchPercentage: toPercentage(rewatches, totalWatches),
  };
};

const buildRewatchByYear = (diaryRows) => {
  const tallies = new Map();

  diaryRows.forEach((row) => {
    const year = extractYear(row);
    if (!year) return;

    const tally = tallies.get(year) || { totalWatches: 0, rewatches: 0 };
    tally.totalWatches += 1;
    if (isRewatchRow(row)) tally.rewatches += 1;
    tallies.set(year, tally);
  });

  return Array.from(tallies.entries())
    .map(toYearStat)
    .sort((a, b) => b.year.localeCompare(a.year));
};

module.exports = {
  buildRewatchByYear,
};
