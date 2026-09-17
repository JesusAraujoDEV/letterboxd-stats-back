const { parseCsvBuffer, getZipEntryBuffer } = require("../utils/csvHelper");

const ZIP_ENTRIES = [
  ["watchedRows", "watched.csv", true],
  ["ratingsRows", "ratings.csv", true],
  ["diaryRows", "diary.csv", true],
  ["profileRows", "profile.csv", false],
  ["watchlistRows", "watchlist.csv", false],
  ["reviewsRows", "reviews.csv", false],
  ["commentsRows", "comments.csv", false],
  ["deletedDiaryRows", "deleted/diary.csv", false],
  ["deletedReviewsRows", "deleted/reviews.csv", false],
  ["deletedCommentsRows", "deleted/comments.csv", false],
  ["likedFilmsRows", "likes/films.csv", false],
  ["likedListsRows", "likes/lists.csv", false],
  ["likedReviewsRows", "likes/reviews.csv", false],
];

const parseZipCsvEntries = async (zip) => {
  const safeParseCsv = async (filename) => {
    try {
      return await parseCsvBuffer(getZipEntryBuffer(zip, filename));
    } catch (err) {
      return [];
    }
  };

  const values = await Promise.all(
    ZIP_ENTRIES.map(([, filename, required]) =>
      required ? parseCsvBuffer(getZipEntryBuffer(zip, filename)) : safeParseCsv(filename),
    ),
  );

  return Object.fromEntries(ZIP_ENTRIES.map(([key], index) => [key, values[index]]));
};

const buildProfile = (profileRows) => {
  const profileRow = profileRows[0] || {};
  return {
    profileRow,
    profile: {
      username: profileRow.Username || "",
      location: profileRow.Location || "",
      bio: profileRow.Bio || "",
    },
  };
};

const buildDaysActive = (profileRow) => {
  const dateJoined = profileRow["Date Joined"] || null;
  return dateJoined ? Math.floor((Date.now() - new Date(dateJoined).getTime()) / 86400000) : null;
};

module.exports = { parseZipCsvEntries, buildProfile, buildDaysActive };
