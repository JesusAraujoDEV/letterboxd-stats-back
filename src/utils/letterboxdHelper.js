const requestOptions = () => ({
  headers: { "User-Agent": "Mozilla/5.0 (compatible; letterboxd-stats/1.0)" },
  signal: AbortSignal.timeout(15000),
});

const fetchFeedXml = async (username) => {
  const response = await fetch(`https://letterboxd.com/${username}/rss/`, requestOptions());
  if (!response.ok) {
    throw new Error(`Feed de Letterboxd respondió ${response.status}.`);
  }
  return response.text();
};

const parseFilmPage = (url, html) => {
  const slug = (url.match(/\/film\/([^/]+)/) || [])[1];
  const tmdb = html.match(/themoviedb\.org\/(movie|tv)\/(\d+)/);
  return {
    slug: slug || null,
    tmdbType: tmdb ? tmdb[1] : "movie",
    tmdbId: tmdb ? Number(tmdb[2]) : null,
  };
};

// ponytail: el HTML del film es la única fuente exacta boxd.it → TMDB; si Letterboxd bloquea, la película queda en `failed`
const resolveFilmUri = async (uri) => {
  const response = await fetch(uri, requestOptions());
  if (!response.ok) {
    throw new Error(`Letterboxd respondió ${response.status} para ${uri}.`);
  }
  return parseFilmPage(response.url, await response.text());
};

module.exports = { fetchFeedXml, parseFilmPage, resolveFilmUri };
