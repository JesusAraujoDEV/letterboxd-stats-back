const buildSearchUrl = (title, year) => {
  const params = new URLSearchParams({
    query: title || "",
    year: year ? String(year) : "",
    language: "en-US",
  });

  return `https://api.themoviedb.org/3/search/movie?${params.toString()}`;
};

const buildDetailsUrl = (movieId) => {
  return `https://api.themoviedb.org/3/movie/${movieId}?language=en-US`;
};

const fetchTmdbJson = async (url) => {
  const token = process.env.TMDB_API_KEY;
  if (!token) {
    return null;
  }

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      return null;
    }

    return await response.json();
  } catch (err) {
    return null;
  }
};

const fetchMovieSearchResult = async (title, year) => {
  if (!title) {
    return null;
  }

  const url = buildSearchUrl(title, year);
  const payload = await fetchTmdbJson(url);
  return payload && payload.results && payload.results[0] ? payload.results[0] : null;
};

const fetchMoviePosterPath = async (title, year) => {
  const result = await fetchMovieSearchResult(title, year);
  return result && result.poster_path ? result.poster_path : null;
};

const fetchMovieDetailsByTitleYear = async (title, year) => {
  const result = await fetchMovieSearchResult(title, year);
  if (!result || !result.id) {
    return null;
  }

  const detailsUrl = buildDetailsUrl(result.id);
  return await fetchTmdbJson(detailsUrl);
};

module.exports = {
  fetchMoviePosterPath,
  fetchMovieDetailsByTitleYear,
};
