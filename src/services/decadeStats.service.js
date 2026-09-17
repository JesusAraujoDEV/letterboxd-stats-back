const { fetchMoviePosterPath } = require("../utils/tmdbHelper");

const MIN_MOVIES_THRESHOLD = 4;

const sortMovies = (a, b) => {
  if (b.userRating !== a.userRating) return b.userRating - a.userRating;
  if (a.ratedDate && b.ratedDate && a.ratedDate !== b.ratedDate) {
    return b.ratedDate.localeCompare(a.ratedDate);
  }
  return (a.title || "").localeCompare(b.title || "");
};

const enrichWithPosterPaths = async (movies) => {
  const batchSize = 5;
  const enriched = movies.map((movie) => ({ ...movie, posterPath: null }));

  for (let i = 0; i < enriched.length; i += batchSize) {
    const batch = enriched.slice(i, i + batchSize);
    const posterPaths = await Promise.all(
      batch.map((movie) => fetchMoviePosterPath(movie.title, movie.year)),
    );
    posterPaths.forEach((posterPath, index) => {
      batch[index].posterPath = posterPath || null;
    });
  }

  return enriched;
};

const buildDecadeMap = (ratingsRows) => {
  const decadeMap = {};

  ratingsRows.forEach((row) => {
    const title = row.Name || row.Title;
    const year = Number(row.Year);
    const rating = parseFloat(row.Rating);
    if (!title || !Number.isFinite(year) || !Number.isFinite(rating)) return;

    const key = String(Math.floor(year / 10) * 10);
    if (!decadeMap[key]) decadeMap[key] = { sum: 0, count: 0, movies: [] };

    decadeMap[key].sum += rating;
    decadeMap[key].count += 1;
    decadeMap[key].movies.push({
      title: String(title).trim(),
      year: String(year),
      userRating: rating,
      ratedDate: row.Date ? String(row.Date).trim() : null,
    });
  });

  return decadeMap;
};

const pickTopDecades = (decadeMap) => {
  const decadeAverages = Object.entries(decadeMap).map(([decade, data]) => ({
    decade: Number(decade),
    average: data.count > 0 ? Number((data.sum / data.count).toFixed(2)) : 0,
    movies: data.movies,
    movieCount: data.count,
  }));

  for (const threshold of [MIN_MOVIES_THRESHOLD, 2, 1]) {
    const valid = decadeAverages.filter((entry) => entry.movieCount >= threshold);
    if (valid.length >= 3 || threshold === 1) {
      return valid.sort((a, b) => b.average - a.average).slice(0, 3);
    }
  }
  return [];
};

const buildTopDecades = async (ratingsRows) => {
  const decadeMap = buildDecadeMap(ratingsRows);
  const topDecadeCandidates = pickTopDecades(decadeMap);

  const topDecades = [];
  for (const entry of topDecadeCandidates) {
    const topMovies = entry.movies.sort(sortMovies).slice(0, 8);
    const topMoviesWithPosters = await enrichWithPosterPaths(topMovies);
    topDecades.push({
      decade: `${entry.decade}s`,
      average: entry.average,
      topMovies: topMoviesWithPosters,
    });
  }

  return topDecades;
};

module.exports = { buildTopDecades };
