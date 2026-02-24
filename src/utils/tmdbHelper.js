const buildSearchUrl = (title, year) => {
  const params = new URLSearchParams({
    query: title || "",
    year: year ? String(year) : "",
    language: "en-US",
  });

  return `https://api.themoviedb.org/3/search/movie?${params.toString()}`;
};

const fetchMoviePosterPath = async (title, year) => {
  const token = process.env.TMDB_API_KEY;
  if (!token || !title) {
    return null;
  }

  const url = buildSearchUrl(title, year);

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

    const payload = await response.json();
    const posterPath = payload && payload.results && payload.results[0] && payload.results[0].poster_path;
    return posterPath || null;
  } catch (err) {
    return null;
  }
};

module.exports = {
  fetchMoviePosterPath,
};
