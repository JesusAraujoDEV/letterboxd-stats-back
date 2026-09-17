const AdmZip = require("adm-zip");
const { parseZipCsvEntries, buildProfile, buildDaysActive } = require("./zipInput.service");
const { buildTopDecades } = require("./decadeStats.service");
const { calculateLongestStreak } = require("./streakStats.service");
const { buildTotalHoursWatched } = require("./watchTimeStats.service");
const { buildMovieMetadata } = require("./movieMetadata.service");
const { buildCastCrewStats } = require("./castCrewStats.service");
const { buildInteractionStats } = require("./interactionStats.service");
const { buildActivityStats } = require("./activityStats.service");
const { buildRatingStats } = require("./ratingStats.service");
const { buildYearTagStats } = require("./yearTagStats.service");
const { buildMostRewatchedMovies } = require("./mostRewatched.service");
const { buildCollectionCounts } = require("./collectionCounts.service");
const {
  buildFranchiseStats,
  buildStudioStats,
  buildIndustryTotals,
  buildRatingComparison,
} = require("./movieExtras.service");
const {
  buildRewatchStats,
  buildReviewTextStats,
  buildRatingExtremes,
  buildRuntimeExtremes,
  buildWatchSpan,
} = require("./diaryExtras.service");
const { buildFavoriteFilms, buildCustomLists } = require("./profileExtras.service");

const buildStatsFromZipBuffer = async (zipBuffer) => {
  let zip;
  try {
    zip = new AdmZip(zipBuffer);
  } catch (err) {
    throw new Error("El archivo no es un ZIP válido.");
  }

  const rows = await parseZipCsvEntries(zip);
  const { profileRow, profile } = buildProfile(rows.profileRows);
  const normalizedMainUsername = profile.username.trim().toLowerCase();

  const { activityStats, watchedYearStats } = buildActivityStats(rows.diaryRows);
  const longestStreak = calculateLongestStreak(
    rows.diaryRows.map((row) => ({ watchedDate: row["Watched Date"] || row.watchedDate || row.Date })),
  );
  const { averageRating, ratingDistribution, averageRatingByReleaseYear } = buildRatingStats(rows.ratingsRows);
  const { topYears, moviesByReleaseYear, topTags } = buildYearTagStats(rows.watchedRows, rows.diaryRows);
  const mostRewatchedMovies = await buildMostRewatchedMovies(rows.diaryRows);
  const topInteractedUsers = await buildInteractionStats(rows.commentsRows, normalizedMainUsername);

  const { likedTitlesSet, counts: collectionCounts } = buildCollectionCounts({ ...rows, zip });

  const tmdbDetailsCache = {};
  const topDecades = await buildTopDecades(rows.ratingsRows);
  const { topGenres, topCountries, topLanguages, allCountries, allMovies } = await buildMovieMetadata(
    rows.watchedRows,
    rows.diaryRows,
    likedTitlesSet,
    tmdbDetailsCache,
  );
  const { topActorsAllTime, topActorsLogged, topDirectorsAllTime, topDirectorsLogged } = buildCastCrewStats(
    allMovies,
    tmdbDetailsCache,
  );
  const totalHoursWatched = await buildTotalHoursWatched(rows.diaryRows, tmdbDetailsCache);

  const favoriteFilms = await buildFavoriteFilms(profileRow);
  const customLists = await buildCustomLists(zip);

  return {
    profile,
    totalMovies: rows.watchedRows.length,
    totalLoggedMovies: rows.diaryRows.length,
    averageRating,
    ratingDistribution,
    topYears,
    moviesByReleaseYear,
    averageRatingByReleaseYear,
    topTags,
    mostRewatchedMovies,
    ...collectionCounts,
    topInteractedUsers,
    longestStreak,
    topDecades,
    topGenres,
    topCountries,
    topLanguages,
    allCountries,
    topActorsAllTime,
    topActorsLogged,
    topDirectorsAllTime,
    topDirectorsLogged,
    totalHoursWatched,
    activityStats,
    watchedYearStats,
    allMovies,
    rewatchStats: buildRewatchStats(rows.diaryRows),
    reviewTextStats: buildReviewTextStats(rows.reviewsRows),
    ratingExtremes: buildRatingExtremes(rows.ratingsRows, tmdbDetailsCache),
    runtimeExtremes: buildRuntimeExtremes(rows.diaryRows, tmdbDetailsCache),
    watchSpan: buildWatchSpan(rows.diaryRows, tmdbDetailsCache),
    franchiseStats: buildFranchiseStats(allMovies),
    studioStats: buildStudioStats(allMovies),
    industryTotals: buildIndustryTotals(allMovies),
    ratingComparison: buildRatingComparison(rows.ratingsRows, tmdbDetailsCache),
    favoriteFilms,
    customLists,
    daysActive: buildDaysActive(profileRow),
  };
};

module.exports = {
  buildStatsFromZipBuffer,
};
