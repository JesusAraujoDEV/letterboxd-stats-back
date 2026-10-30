const test = require("node:test");
const assert = require("node:assert/strict");
const { parseFeed } = require("./feedParser.service");
const { buildSnapshot } = require("./zipSnapshot.service");
const { rankCandidates } = require("./recommendationRanking.service");
const { isSyncDue } = require("./syncScheduler.service");
const { parseJustWatchNode, findJustWatchMatch } = require("./netflixAvailability.service");
const { parseFilmPage } = require("../utils/letterboxdHelper");
const { netflixCountriesFromProviders } = require("../utils/tmdbHelper");

const FEED = `<?xml version='1.0'?><rss xmlns:letterboxd="https://letterboxd.com" xmlns:tmdb="https://themoviedb.org"><channel>
<item><title>Newer</title><link>https://letterboxd.com/u/film/companion-2025/1/</link>
<letterboxd:watchedDate>2026-10-28</letterboxd:watchedDate><letterboxd:rewatch>Yes</letterboxd:rewatch>
<letterboxd:filmTitle>Companion</letterboxd:filmTitle><letterboxd:filmYear>2025</letterboxd:filmYear>
<letterboxd:memberRating>5.0</letterboxd:memberRating><letterboxd:memberLike>Yes</letterboxd:memberLike>
<tmdb:movieId>1084199</tmdb:movieId>
<description><![CDATA[ <p><img src="x.jpg"/></p> <p>I love this movie</p> ]]></description></item>
<item><title>Older</title><link>https://letterboxd.com/u/film/creed/</link>
<letterboxd:filmTitle>Creed</letterboxd:filmTitle><letterboxd:filmYear>2015</letterboxd:filmYear>
<letterboxd:rewatch>No</letterboxd:rewatch><tmdb:tvId>7</tmdb:tvId><description></description></item>
<item><title>A list</title><link>https://letterboxd.com/u/list/best/</link></item>
</channel></rss>`;

test("parseFeed ignora listas, ordena de antiguo a nuevo y lee los campos", () => {
  const [older, newer, ...rest] = parseFeed(FEED);
  assert.equal(rest.length, 0);
  assert.equal(older.slug, "creed");
  assert.equal(older.rating, null);
  assert.equal(older.watchedOn, null);
  assert.deepEqual([older.tmdbId, older.tmdbType], [7, "tv"]);
  assert.equal(newer.slug, "companion-2025");
  assert.deepEqual([newer.rating, newer.liked, newer.rewatch], [5, true, true]);
  assert.equal(newer.tmdbId, 1084199);
  assert.equal(newer.review, "I love this movie");
});

test("buildSnapshot une CSV por película y fusiona diario con reseñas", () => {
  const row = { Name: "Nobody", Year: "2021", "Letterboxd URI": "https://boxd.it/film" };
  const entry = { Name: "Nobody", Year: "2021", "Letterboxd URI": "https://boxd.it/entry", "Watched Date": "2024-01-03" };
  const snapshot = buildSnapshot({
    watchedRows: [row],
    ratingsRows: [{ ...row, Rating: "4.5" }],
    likedFilmsRows: [row],
    watchlistRows: [{ Name: "Other", Year: "1999", "Letterboxd URI": "https://boxd.it/other" }],
    diaryRows: [{ ...entry, Rating: "4.5", Tags: "netflix" }, { Name: "Orphan", Year: "2025", "Letterboxd URI": "", "Watched Date": "2025-04-20" }],
    reviewsRows: [{ ...entry, Rating: "4.5", Review: "Great" }],
  });

  assert.equal(snapshot.films.length, 2);
  const nobody = snapshot.films.find((film) => film.title === "Nobody");
  assert.equal(nobody.uri, "https://boxd.it/film");
  assert.deepEqual([nobody.rating, nobody.watched, nobody.liked], [4.5, true, true]);
  assert.equal(snapshot.films.find((film) => film.title === "Other").inWatchlist, true);
  assert.equal(snapshot.diary.length, 2);
  assert.deepEqual([snapshot.diary[0].tags, snapshot.diary[0].review], ["netflix", "Great"]);
  assert.equal(snapshot.cutoff, "2025-04-20");
});

test("rankCandidates excluye lo visto, acumula puntaje y da el motivo de la mejor semilla", () => {
  const movie = (id) => ({ id, title: `M${id}`, release_date: "2020-05-01", poster_path: "/p.jpg" });
  const ranked = rankCandidates(
    [
      { seed: { title: "A", rating: 5 }, results: [movie(1), movie(2), movie(3)] },
      { seed: { title: "B", rating: 4 }, results: [movie(2), { ...movie(4), adult: true }] },
    ],
    new Set([1]),
  );

  assert.deepEqual(ranked.map((item) => item.tmdbId), [2, 3]);
  assert.deepEqual([ranked[0].score, ranked[0].reason, ranked[0].year], [9, "Porque te encantó A", 2020]);
});

test("isSyncDue corre a la hora fijada y recupera corridas perdidas", () => {
  const at = (iso) => new Date(iso);
  assert.equal(isSyncDue(at("2026-10-30T12:00:00Z"), null), true);
  assert.equal(isSyncDue(at("2026-10-30T07:30:00Z"), at("2026-10-29T07:10:00Z")), true);
  assert.equal(isSyncDue(at("2026-10-30T07:30:00Z"), at("2026-10-30T07:05:00Z")), false);
  assert.equal(isSyncDue(at("2026-10-30T12:00:00Z"), at("2026-10-30T07:05:00Z")), false);
  assert.equal(isSyncDue(at("2026-10-30T12:00:00Z"), at("2026-10-29T09:00:00Z")), true);
});

test("parseJustWatchNode detecta Netflix por país e ignora otras ofertas", () => {
  const netflix = {
    monetizationType: "FLATRATE",
    standardWebURL: "https://www.netflix.com/title/80175798",
    package: { technicalName: "netflix" },
  };
  const rental = { monetizationType: "RENT", standardWebURL: "https://x.test", package: { technicalName: "netflix" } };
  const other = { monetizationType: "FLATRATE", standardWebURL: "https://hulu.test", package: { technicalName: "hulu" } };

  assert.deepEqual(parseJustWatchNode({ ve: [netflix], us: [netflix] }), {
    netflixCountries: ["VE", "US"],
    netflixId: "80175798",
  });
  assert.deepEqual(parseJustWatchNode({ ve: [], us: [other, netflix] }).netflixCountries, ["US"]);
  assert.deepEqual(parseJustWatchNode({ ve: [rental], us: [other] }), { netflixCountries: [], netflixId: null });
});

test("findJustWatchMatch solo acepta el mismo tmdbId", () => {
  const nodes = [{ content: { externalIds: { tmdbId: "51209" } } }, { content: { externalIds: { tmdbId: "398978" } } }];
  assert.equal(findJustWatchMatch(nodes, 398978), nodes[1]);
  assert.equal(findJustWatchMatch(nodes, 1), undefined);
});

test("netflixCountriesFromProviders usa los proveedores de suscripción de Netflix", () => {
  const results = {
    VE: { flatrate: [{ provider_id: 8 }] },
    US: { flatrate: [{ provider_id: 15 }], rent: [{ provider_id: 8 }] },
  };
  assert.deepEqual(netflixCountriesFromProviders(results), ["VE"]);
  assert.deepEqual(netflixCountriesFromProviders({ US: { flatrate: [{ provider_id: 1796 }] } }), ["US"]);
  assert.deepEqual(netflixCountriesFromProviders(null), []);
});

test("parseFilmPage extrae slug y TMDB de la URL final y el HTML", () => {
  const film = parseFilmPage("https://letterboxd.com/film/creed/", '<a href="https://www.themoviedb.org/movie/312221/">');
  assert.deepEqual(film, { slug: "creed", tmdbType: "movie", tmdbId: 312221 });
});
