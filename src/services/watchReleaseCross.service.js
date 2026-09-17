// Cruces "año que viste" x "año de estreno" — de qué año son las pelis que ves, qué tan
// al día vas, y si con los años te acercas o alejas de los estrenos.

const buildWatchYearBuckets = (diaryRows, bucketFn) => {
  const byWatchYear = {};

  diaryRows.forEach((row) => {
    const watchedDateValue = row["Watched Date"] || row.Date;
    if (!watchedDateValue) return;
    const watchYear = String(watchedDateValue).trim().substring(0, 4);
    if (!/^[0-9]{4}$/.test(watchYear)) return;

    const releaseYear = row.Year ? String(row.Year).trim() : "";
    if (!releaseYear) return;

    if (!byWatchYear[watchYear]) byWatchYear[watchYear] = [];
    byWatchYear[watchYear].push(bucketFn(watchYear, releaseYear));
  });

  return byWatchYear;
};

const buildWatchReleaseCrossStats = (diaryRows, topN = 5) => {
  const buckets = buildWatchYearBuckets(diaryRows, (_watchYear, releaseYear) => releaseYear);

  return Object.entries(buckets)
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([watchYear, releaseYears]) => {
      const counter = {};
      releaseYears.forEach((year) => (counter[year] = (counter[year] || 0) + 1));
      return {
        watchYear,
        topReleaseYears: Object.entries(counter)
          .sort((a, b) => b[1] - a[1])
          .slice(0, topN)
          .map(([releaseYear, count]) => ({ releaseYear, count })),
      };
    });
};

const buildWatchAgeGapStats = (diaryRows) => {
  const buckets = buildWatchYearBuckets(diaryRows, (watchYear, releaseYear) => {
    const gap = Number(watchYear) - Number(releaseYear);
    return Number.isFinite(gap) && gap >= 0 ? gap : null;
  });

  return Object.entries(buckets)
    .map(([watchYear, gaps]) => {
      const validGaps = gaps.filter((gap) => gap != null);
      if (validGaps.length === 0) return null;
      const average = validGaps.reduce((sum, gap) => sum + gap, 0) / validGaps.length;
      return { watchYear, averageAgeYears: Number(average.toFixed(1)) };
    })
    .filter(Boolean)
    .sort((a, b) => b.watchYear.localeCompare(a.watchYear));
};

const buildDominantDecadeByWatchYear = (diaryRows) => {
  const buckets = buildWatchYearBuckets(diaryRows, (_watchYear, releaseYear) => {
    const year = Number(releaseYear);
    return Number.isFinite(year) ? `${Math.floor(year / 10) * 10}s` : null;
  });

  return Object.entries(buckets)
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([watchYear, decades]) => {
      const valid = decades.filter(Boolean);
      if (valid.length === 0) return null;

      const counter = {};
      valid.forEach((decade) => (counter[decade] = (counter[decade] || 0) + 1));
      const [dominantDecade, count] = Object.entries(counter).sort((a, b) => b[1] - a[1])[0];

      return {
        watchYear,
        dominantDecade,
        count,
        percentage: Number(((count / valid.length) * 100).toFixed(1)),
      };
    })
    .filter(Boolean);
};

const buildPremiereChaserStats = (diaryRows) => {
  const buckets = buildWatchYearBuckets(diaryRows, (watchYear, releaseYear) => releaseYear === watchYear);

  return Object.entries(buckets)
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([watchYear, isPremiereFlags]) => {
      const premieresWatched = isPremiereFlags.filter(Boolean).length;
      return {
        watchYear,
        premieresWatched,
        percentage: Number(((premieresWatched / isPremiereFlags.length) * 100).toFixed(1)),
      };
    });
};

module.exports = {
  buildWatchReleaseCrossStats,
  buildWatchAgeGapStats,
  buildDominantDecadeByWatchYear,
  buildPremiereChaserStats,
};
