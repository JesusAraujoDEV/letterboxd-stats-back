const { syncFeed } = require("./feedSync.service");
const { getLastFeedSuccess } = require("./syncRun.service");

const HOUR_MS = 60 * 60 * 1000;
const RUN_HOUR_UTC = 7; // 03:00 en Caracas (UTC-4, sin horario de verano)

// Corre a la hora fijada; si una corrida se perdió o falló, reintenta cada hora tras 26 h.
const isSyncDue = (now, lastSuccess) => {
  if (!lastSuccess) {
    return true;
  }
  const age = now.getTime() - new Date(lastSuccess).getTime();
  return age >= 26 * HOUR_MS || (now.getUTCHours() === RUN_HOUR_UTC && age >= 20 * HOUR_MS);
};

const tick = async () => {
  try {
    if (isSyncDue(new Date(), await getLastFeedSuccess())) {
      console.log("Sync de Letterboxd:", await syncFeed());
    }
  } catch (err) {
    console.error("Sync de Letterboxd falló:", err.message);
  }
};

// ponytail: temporizador dentro del proceso, asume una sola réplica; con más, mover a un cron externo
const startSyncScheduler = () => {
  tick();
  setInterval(tick, HOUR_MS);
};

module.exports = { isSyncDue, startSyncScheduler };
