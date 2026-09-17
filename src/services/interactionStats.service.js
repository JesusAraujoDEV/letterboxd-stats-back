const { fetchMoviePosterPath } = require("../utils/tmdbHelper");
const { mapWithConcurrency } = require("../utils/concurrency");

const letterboxdLinkCache = new Map();

const resolveLetterboxdLink = async (shortUrl) => {
  if (letterboxdLinkCache.has(shortUrl)) return letterboxdLinkCache.get(shortUrl);

  try {
    const response = await fetch(shortUrl, {
      method: "HEAD",
      redirect: "follow",
      signal: AbortSignal.timeout(4000),
    });
    const finalUrlObj = new URL(response.url);
    const pathParts = finalUrlObj.pathname.split("/").filter(Boolean);
    const username = pathParts[0] || "unknown";
    const slug = pathParts[2] || "";
    const itemName = slug
      ? slug.split("-").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ")
      : "";

    const result = { username, itemName, finalUrl: response.url };
    letterboxdLinkCache.set(shortUrl, result);
    return result;
  } catch (error) {
    console.error(`Error resolviendo ${shortUrl}:`, error.message);
    const fallback = { username: "unknown", itemName: "", finalUrl: "" };
    letterboxdLinkCache.set(shortUrl, fallback);
    return fallback;
  }
};

const buildInteractionsMap = (commentsWithLinks, resolvedLinks, normalizedMainUsername) => {
  const interactionsMap = new Map();

  commentsWithLinks.forEach((row, index) => {
    const { username: resolvedUsername, itemName, finalUrl } = resolvedLinks[index];
    const normalizedUsername = resolvedUsername ? String(resolvedUsername).trim().toLowerCase() : "";
    if (resolvedUsername === "unknown" || (normalizedMainUsername && normalizedUsername === normalizedMainUsername)) {
      return;
    }

    if (!interactionsMap.has(resolvedUsername)) {
      interactionsMap.set(resolvedUsername, { username: resolvedUsername, interactionCount: 0, comments: [] });
    }
    const userStats = interactionsMap.get(resolvedUsername);
    userStats.interactionCount += 1;
    userStats.comments.push({
      date: row.Date || null,
      text: row.Comment || "",
      movie: itemName,
      finalUrl,
    });
  });

  return interactionsMap;
};

const attachCommentPosters = async (top10Users) => {
  const uniqueMovies = new Set();
  top10Users.forEach((user) => user.comments.forEach((c) => c.movie && uniqueMovies.add(c.movie)));

  const posterCache = new Map();
  await mapWithConcurrency(Array.from(uniqueMovies), 8, async (movie) => {
    const posterPath = await fetchMoviePosterPath(movie, null);
    posterCache.set(movie, posterPath ? `https://image.tmdb.org/t/p/w200${posterPath}` : null);
  });

  top10Users.forEach((user) => {
    user.comments.forEach((comment) => {
      if (comment.movie) comment.posterUrl = posterCache.get(comment.movie) ?? null;
    });
  });
};

const buildInteractionStats = async (commentsRows, normalizedMainUsername) => {
  const commentsWithLinks = commentsRows.filter((row) => row.Content && String(row.Content).includes("boxd.it"));
  const resolvedLinks = await mapWithConcurrency(commentsWithLinks, 8, (row) =>
    resolveLetterboxdLink(String(row.Content).trim()),
  );

  const interactionsMap = buildInteractionsMap(commentsWithLinks, resolvedLinks, normalizedMainUsername);
  const topInteractedUsers = Array.from(interactionsMap.values()).sort(
    (a, b) => b.interactionCount - a.interactionCount,
  );

  // ponytail: scraping de avatares desactivado a propósito para reducir CPU y latencia del endpoint
  topInteractedUsers.slice(0, 15).forEach((user) => (user.avatarUrl = null));
  await attachCommentPosters(topInteractedUsers.slice(0, 10));

  return topInteractedUsers;
};

module.exports = { buildInteractionStats };
