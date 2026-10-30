const { withTransaction } = require("../utils/db");
const { fetchFeedXml } = require("../utils/letterboxdHelper");
const { parseFeed } = require("./feedParser.service");
const { upsertFilm, upsertDiaryEntry, fillMissingPosters } = require("./libraryStore.service");
const { refreshRecommendations } = require("./recommendations.service");
const { recordRun } = require("./syncRun.service");

const applyItem = async (client, item) => {
  await upsertFilm(client, { ...item, watched: true });
  await client.query("UPDATE letterboxd.films SET in_watchlist = false WHERE slug = $1", [item.slug]);
  if (item.watchedOn) {
    await upsertDiaryEntry(client, item);
  }
};

const syncFeed = () =>
  recordRun("feed", async () => {
    const items = parseFeed(await fetchFeedXml(process.env.LETTERBOXD_USERNAME));
    if (!items.length) {
      throw new Error("El feed de Letterboxd no trajo películas.");
    }

    await withTransaction(async (client) => {
      for (const item of items) {
        await applyItem(client, item);
      }
    });
    await fillMissingPosters();
    return { items: items.length, recommendations: await refreshRecommendations() };
  });

module.exports = { syncFeed };
