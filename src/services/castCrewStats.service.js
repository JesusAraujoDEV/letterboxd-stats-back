const { buildTmdbCacheKey } = require("../utils/cacheKey");

const incrementByMovie = (counter, name, profilePath) => {
  const safeName = name ? String(name).trim() : "";
  if (!safeName) return;

  if (!counter[safeName]) {
    counter[safeName] = { name: safeName, count: 1, profilePath: profilePath || null };
    return;
  }
  counter[safeName].count += 1;
  if (!counter[safeName].profilePath && profilePath) counter[safeName].profilePath = profilePath;
};

const buildProfileMaps = (movie, detailsCache) => {
  const movieYear = Number.isFinite(movie.releaseYear) ? String(movie.releaseYear) : "";
  const cacheKey = movie.title ? buildTmdbCacheKey(movie.title, movieYear) : "";
  const credits = (cacheKey && detailsCache[cacheKey] && detailsCache[cacheKey].credits) || {};
  const cast = Array.isArray(credits.cast) ? credits.cast : [];
  const crew = Array.isArray(credits.crew) ? credits.crew : [];

  return {
    castProfileByName: new Map(
      cast.filter((m) => m && m.name).map((m) => [m.name, m.profile_path || null]),
    ),
    directorProfileByName: new Map(
      crew.filter((m) => m && m.job === "Director" && m.name).map((m) => [m.name, m.profile_path || null]),
    ),
  };
};

const topN = (counter, n = 10) =>
  Object.values(counter).sort((a, b) => b.count - a.count).slice(0, n);

const buildCastCrewStats = (allMovies, detailsCache) => {
  const actorsAllTime = {};
  const directorsAllTime = {};
  const actorsLogged = {};
  const directorsLogged = {};

  allMovies.forEach((movie) => {
    const { castProfileByName, directorProfileByName } = buildProfileMaps(movie, detailsCache);
    const uniqueCast = new Set((movie.cast || []).filter(Boolean));
    const uniqueDirectors = new Set((movie.directors || []).filter(Boolean));
    const hasLogs = Array.isArray(movie.diaryLogs) && movie.diaryLogs.length > 0;

    uniqueCast.forEach((name) => incrementByMovie(actorsAllTime, name, castProfileByName.get(name)));
    uniqueDirectors.forEach((name) => incrementByMovie(directorsAllTime, name, directorProfileByName.get(name)));

    if (hasLogs) {
      uniqueCast.forEach((name) => incrementByMovie(actorsLogged, name, castProfileByName.get(name)));
      uniqueDirectors.forEach((name) => incrementByMovie(directorsLogged, name, directorProfileByName.get(name)));
    }
  });

  return {
    topActorsAllTime: topN(actorsAllTime),
    topDirectorsAllTime: topN(directorsAllTime),
    topActorsLogged: topN(actorsLogged),
    topDirectorsLogged: topN(directorsLogged),
  };
};

module.exports = { buildCastCrewStats };
