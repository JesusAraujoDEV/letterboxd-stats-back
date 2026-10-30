const MAX_CANDIDATES = 150;

// `seedResults`: pares { seed, results } con las semillas ya ordenadas de mejor a peor.
const rankCandidates = (seedResults, seenTmdbIds) => {
  const candidates = new Map();

  seedResults.forEach(({ seed, results }) => {
    results
      .filter((movie) => !movie.adult && !seenTmdbIds.has(movie.id))
      .forEach((movie) => {
        const current = candidates.get(movie.id);
        if (current) {
          current.score += seed.rating;
          return;
        }
        candidates.set(movie.id, {
          tmdbId: movie.id,
          title: movie.title,
          year: Number((movie.release_date || "").slice(0, 4)) || null,
          posterPath: movie.poster_path || null,
          reason: `Porque te encantó ${seed.title}`,
          score: seed.rating,
        });
      });
  });

  return [...candidates.values()].sort((a, b) => b.score - a.score).slice(0, MAX_CANDIDATES);
};

module.exports = { rankCandidates };
