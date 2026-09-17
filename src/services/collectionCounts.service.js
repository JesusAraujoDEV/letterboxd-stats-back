const { toTopN } = require("../utils/csvHelper");

const buildDeletedListsInfo = (zip) => {
  const entries = zip
    .getEntries()
    .filter(
      (entry) =>
        entry.entryName.toLowerCase().startsWith("deleted/lists/") &&
        entry.entryName.toLowerCase().endsWith(".csv"),
    );

  return {
    deletedListsCount: entries.length,
    deletedListsNames: entries.map((entry) => {
      const baseName = entry.entryName.split("/").pop() || "";
      return baseName.replace(/\.csv$/i, "").replace(/-/g, " ").trim();
    }),
  };
};

const buildLikedStats = (likedFilmsRows) => {
  const likedYearCounter = {};
  const likedTitlesSet = new Set();

  likedFilmsRows.forEach((row) => {
    const title = row.Name || row.Title;
    if (title) likedTitlesSet.add(String(title).trim());
    if (row.Year) likedYearCounter[String(row.Year).trim()] = (likedYearCounter[String(row.Year).trim()] || 0) + 1;
  });

  return { likedTitlesSet, topLikedYears: toTopN(likedYearCounter, 3, "year") };
};

const buildCollectionCounts = ({
  watchlistRows,
  reviewsRows,
  commentsRows,
  deletedDiaryRows,
  deletedReviewsRows,
  deletedCommentsRows,
  likedFilmsRows,
  likedListsRows,
  likedReviewsRows,
  zip,
}) => {
  const { likedTitlesSet, topLikedYears } = buildLikedStats(likedFilmsRows);

  return {
    likedTitlesSet,
    counts: {
      totalWatchlist: watchlistRows.length,
      totalReviews: reviewsRows.length,
      totalComments: commentsRows.length,
      deletedDiaryCount: deletedDiaryRows.length,
      deletedReviewsCount: deletedReviewsRows.length,
      deletedCommentsCount: deletedCommentsRows.length,
      ...buildDeletedListsInfo(zip),
      totalLikedFilms: likedFilmsRows.length,
      totalLikedLists: likedListsRows.length,
      totalLikedReviews: likedReviewsRows.length,
      topLikedYears,
    },
  };
};

module.exports = { buildCollectionCounts };
