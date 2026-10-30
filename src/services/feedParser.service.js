const cheerio = require("cheerio");

const parseReview = (descriptionHtml) => {
  const $ = cheerio.load(descriptionHtml || "");
  const text = $("p:not(:has(img))")
    .map((_, el) => $(el).text().trim())
    .get()
    .filter((line) => line && !line.startsWith("Watched on "))
    .join("\n");
  return text || null;
};

const parseItem = ($, item) => {
  const field = (tag) => $(item).find(tag.replace(":", "\\:")).first().text().trim();
  const slug = (field("link").match(/\/film\/([^/]+)/) || [])[1];
  const title = field("letterboxd:filmTitle");
  if (!slug || !title) {
    return null;
  }

  const movieId = field("tmdb:movieId");
  const tvId = field("tmdb:tvId");
  return {
    slug,
    title,
    year: Number(field("letterboxd:filmYear")) || null,
    rating: Number(field("letterboxd:memberRating")) || null,
    liked: field("letterboxd:memberLike") === "Yes",
    rewatch: field("letterboxd:rewatch") === "Yes",
    watchedOn: field("letterboxd:watchedDate") || null,
    tmdbId: Number(movieId || tvId) || null,
    tmdbType: tvId ? "tv" : "movie",
    review: parseReview(field("description")),
  };
};

// De más antiguo a más nuevo: la entrada más reciente gana en los upserts.
const parseFeed = (xml) => {
  const $ = cheerio.load(xml, { xmlMode: true });
  return $("item")
    .map((_, item) => parseItem($, item))
    .get()
    .filter(Boolean)
    .reverse();
};

module.exports = { parseFeed };
