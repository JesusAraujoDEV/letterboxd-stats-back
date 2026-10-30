const { fetchJustWatchTitles } = require("../utils/justwatchHelper");
const { fetchNetflixCountries } = require("../utils/tmdbHelper");

const NETFLIX_PACKAGES = new Set(["netflix", "netflixbasicwithads"]);

const findNetflixOffer = (offers) =>
  (offers || []).find(
    (offer) =>
      offer.monetizationType === "FLATRATE" && NETFLIX_PACKAGES.has(offer.package.technicalName),
  );

const parseJustWatchNode = (node) => {
  const ve = findNetflixOffer(node.ve);
  const us = findNetflixOffer(node.us);
  const offer = ve || us;
  const url = offer ? offer.standardWebURL || "" : "";
  return {
    netflixCountries: [ve && "VE", us && "US"].filter(Boolean),
    netflixId: (url.match(/\/title\/(\d+)/) || [])[1] || null,
  };
};

const findJustWatchMatch = (nodes, tmdbId) =>
  nodes.find((node) => Number(node.content.externalIds.tmdbId) === tmdbId);

const fetchFromJustWatch = async ({ tmdbId, title }) => {
  try {
    const match = findJustWatchMatch(await fetchJustWatchTitles(title), tmdbId);
    return match ? parseJustWatchNode(match) : null;
  } catch (err) {
    console.warn(`JustWatch falló para ${title}:`, err.message);
    return null;
  }
};

const fetchNetflixAvailability = async (movie) => {
  const fromJustWatch = await fetchFromJustWatch(movie);
  if (fromJustWatch) {
    return fromJustWatch;
  }
  return { netflixCountries: await fetchNetflixCountries(movie.tmdbId), netflixId: null };
};

module.exports = { parseJustWatchNode, findJustWatchMatch, fetchNetflixAvailability };
