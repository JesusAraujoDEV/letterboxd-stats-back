const buildRatingDistribution = (ratingsRows) => {
  let ratingSum = 0;
  let ratingCount = 0;
  const ratingDistribution = {};

  ratingsRows.forEach((row) => {
    const rating = parseFloat(row.Rating);
    if (!Number.isFinite(rating)) return;
    ratingSum += rating;
    ratingCount += 1;
    const key = rating.toString();
    ratingDistribution[key] = (ratingDistribution[key] || 0) + 1;
  });

  return {
    ratingDistribution,
    averageRating: ratingCount > 0 ? Number((ratingSum / ratingCount).toFixed(2)) : 0,
  };
};

const buildAverageRatingByReleaseYear = (ratingsRows) => {
  const ratingYearMap = {};

  ratingsRows.forEach((row) => {
    const year = Number(row.Year);
    const rating = parseFloat(row.Rating);
    if (!Number.isFinite(year) || !Number.isFinite(rating)) return;
    const key = String(year);
    if (!ratingYearMap[key]) ratingYearMap[key] = { sum: 0, count: 0 };
    ratingYearMap[key].sum += rating;
    ratingYearMap[key].count += 1;
  });

  const years = Object.keys(ratingYearMap).map(Number).filter(Number.isFinite);
  if (years.length === 0) return [];

  const minYear = Math.min(...years);
  const maxYear = Math.max(...years);
  const result = [];
  for (let year = minYear; year <= maxYear; year += 1) {
    const entry = ratingYearMap[String(year)];
    const average = entry && entry.count > 0 ? Number((entry.sum / entry.count).toFixed(2)) : 0;
    result.push({ year: String(year), average });
  }
  return result;
};

const buildRatingStats = (ratingsRows) => ({
  ...buildRatingDistribution(ratingsRows),
  averageRatingByReleaseYear: buildAverageRatingByReleaseYear(ratingsRows),
});

module.exports = { buildRatingStats };
