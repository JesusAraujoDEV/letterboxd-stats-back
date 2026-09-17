// ponytail: límite manual de concurrencia (sin dependencia nueva) — límite fijo por llamada, ajustar si el perfil de tráfico cambia
const mapWithConcurrency = async (items, limit, fn) => {
  const results = new Array(items.length);
  let index = 0;

  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (index < items.length) {
      const current = index++;
      results[current] = await fn(items[current], current);
    }
  });

  await Promise.all(workers);
  return results;
};

module.exports = { mapWithConcurrency };
