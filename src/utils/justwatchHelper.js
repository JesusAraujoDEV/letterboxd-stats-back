const JUSTWATCH_URL = "https://apis.justwatch.com/graphql";

const TITLES_QUERY = `query Titles($country: Country!, $language: Language!, $filter: TitleFilter!) {
  popularTitles(country: $country, first: 10, filter: $filter) {
    edges {
      node {
        content(country: $country, language: $language) { externalIds { tmdbId } }
        ve: offers(country: VE, platform: WEB) { ...OfferFields }
        us: offers(country: US, platform: WEB) { ...OfferFields }
      }
    }
  }
}
fragment OfferFields on Offer {
  monetizationType
  standardWebURL
  package { technicalName }
}`;

// ponytail: API no oficial de JustWatch; si cambia, netflixAvailability cae a TMDB (ADR 0002)
const fetchJustWatchTitles = async (title) => {
  const response = await fetch(JUSTWATCH_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json", "User-Agent": "Mozilla/5.0" },
    body: JSON.stringify({
      query: TITLES_QUERY,
      variables: { country: "US", language: "en", filter: { searchQuery: title } },
    }),
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok) {
    throw new Error(`JustWatch respondió ${response.status}.`);
  }

  const payload = await response.json();
  if (!payload.data) {
    throw new Error("JustWatch respondió sin datos.");
  }
  return payload.data.popularTitles.edges.map((edge) => edge.node);
};

module.exports = { fetchJustWatchTitles };
