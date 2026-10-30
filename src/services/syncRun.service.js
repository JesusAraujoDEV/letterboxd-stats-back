const { query } = require("../utils/db");

const finishRun = (id, ok, error, summary) =>
  query(
    `UPDATE letterboxd.sync_runs
     SET finished_at = now(), ok = $2, error = $3, summary = $4
     WHERE id = $1`,
    [id, ok, error, summary],
  );

const recordRun = async (source, fn) => {
  const { rows } = await query(
    "INSERT INTO letterboxd.sync_runs (source) VALUES ($1) RETURNING id",
    [source],
  );

  try {
    const summary = await fn();
    await finishRun(rows[0].id, true, null, summary);
    return summary;
  } catch (err) {
    await finishRun(rows[0].id, false, err.message, null);
    throw err;
  }
};

const getLastFeedSuccess = async () => {
  const { rows } = await query(
    `SELECT max(finished_at) AS finished_at FROM letterboxd.sync_runs
     WHERE source = 'feed' AND ok`,
  );
  return rows[0].finished_at;
};

const getHealth = async () => {
  const { rows } = await query(
    `SELECT
       (SELECT max(finished_at) FROM letterboxd.sync_runs WHERE ok) AS last_update,
       last.source, last.ok, last.error, last.finished_at
     FROM (SELECT * FROM letterboxd.sync_runs ORDER BY started_at DESC LIMIT 1) last`,
  );
  const run = rows[0];
  if (!run) {
    return { lastUpdate: null, failed: false, lastRun: null };
  }

  return {
    lastUpdate: run.last_update,
    failed: run.ok === false,
    lastRun: { source: run.source, ok: run.ok, error: run.error, finishedAt: run.finished_at },
  };
};

module.exports = { recordRun, getLastFeedSuccess, getHealth };
