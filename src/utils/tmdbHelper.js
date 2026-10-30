const buildSearchUrl = (title, year) => {
  const params = new URLSearchParams({
    query: title || "",
    year: year ? String(year) : "",
    language: "en-US",
  });

  return `https://api.themoviedb.org/3/search/movie?${params.toString()}`;
};

const buildDetailsUrl = (movieId) => {
  const params = new URLSearchParams({
    language: "en-US",
    append_to_response: "credits",
  });
  return `https://api.themoviedb.org/3/movie/${movieId}?${params.toString()}`;
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
      signal: AbortSignal.timeout(5000),
    });

    if (!response.ok) {
      if (response.status === 429) {
        console.warn(`TMDB rate limit (429): ${url}`);
      }
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

const fetchTmdbTitle = (type, id) =>
  fetchTmdbJson(`https://api.themoviedb.org/3/${type}/${id}?language=en-US`);

const fetchMovieRecommendations = async (id) => {
  const url = `https://api.themoviedb.org/3/movie/${id}/recommendations?language=en-US&page=1`;
  const payload = await fetchTmdbJson(url);
  return payload && payload.results ? payload.results : [];
};

const NETFLIX_PROVIDER_IDS = new Set([8, 1796]);

const netflixCountriesFromProviders = (results) =>
  ["VE", "US"].filter((country) => {
    const region = results ? results[country] : null;
    const flatrate = region && region.flatrate ? region.flatrate : [];
    return flatrate.some((provider) => NETFLIX_PROVIDER_IDS.has(provider.provider_id));
  });

const fetchNetflixCountries = async (id) => {
  const payload = await fetchTmdbJson(`https://api.themoviedb.org/3/movie/${id}/watch/providers`);
  return netflixCountriesFromProviders(payload ? payload.results : null);
};

module.exports = {
  fetchMoviePosterPath,
  fetchMovieDetailsByTitleYear,
  fetchTmdbTitle,
  fetchMovieRecommendations,
  netflixCountriesFromProviders,
  fetchNetflixCountries,
};
