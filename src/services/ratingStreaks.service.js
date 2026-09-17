const HIGH_RATING_THRESHOLD = 4;
const LOW_RATING_THRESHOLD = 2;

const buildRatingStreaks = (diaryRows) => {
  const rated = diaryRows
    .map((row) => ({
      date: row["Watched Date"] || row.Date || "",
      rating: parseFloat(row.Rating),
    }))
    .filter((entry) => entry.date && Number.isFinite(entry.rating))
    .sort((a, b) => a.date.localeCompare(b.date));

  let longestHighRatedStreak = 0;
  let currentHighStreak = 0;
  let longestLowRatedStreak = 0;
  let currentLowStreak = 0;

  rated.forEach((entry) => {
    currentHighStreak = entry.rating >= HIGH_RATING_THRESHOLD ? currentHighStreak + 1 : 0;
    longestHighRatedStreak = Math.max(longestHighRatedStreak, currentHighStreak);

    currentLowStreak = entry.rating <= LOW_RATING_THRESHOLD ? currentLowStreak + 1 : 0;
    longestLowRatedStreak = Math.max(longestLowRatedStreak, currentLowStreak);
  });

  return { longestHighRatedStreak, longestLowRatedStreak };
};

module.exports = { buildRatingStreaks };
