const csvParser = require("csv-parser");
const { Readable } = require("stream");

const parseCsvBuffer = (buffer) => {
  return new Promise((resolve, reject) => {
    const rows = [];
    const stream = Readable.from(buffer.toString("utf8"));

    stream
      .pipe(csvParser())
      .on("data", (data) => rows.push(data))
      .on("end", () => resolve(rows))
      .on("error", (err) => reject(err));
  });
};

const getZipEntryBuffer = (zip, filename) => {
  // ponytail: match exacto primero — "diary.csv" no debe resolver a "deleted/diary.csv"
  // solo porque el orden interno del ZIP lo pone antes (endsWith puro es ambiguo aquí)
  const normalized = filename.toLowerCase();
  const entries = zip.getEntries();
  const entry =
    entries.find((e) => e.entryName.toLowerCase() === normalized) ||
    entries.find((e) => e.entryName.toLowerCase().endsWith(`/${normalized}`));

  if (!entry) {
    throw new Error(`Archivo ${filename} no encontrado en el ZIP.`);
  }

  return entry.getData();
};

const toTopN = (counter, n, keyName) => {
  return Object.entries(counter)
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([key, count]) => ({ [keyName]: key, count }));
};

// ponytail: las listas de Letterboxd exportan dos tablas CSV en un solo archivo (metadata + películas), separadas por línea en blanco
const parseListCsvBuffer = async (buffer) => {
  const text = buffer.toString("utf8");
  const filmsIndex = text.indexOf("Position,Name,Year,URL");
  if (filmsIndex === -1) {
    return { name: "", description: "", films: [] };
  }

  const metaLines = text.slice(0, filmsIndex).trim().split("\n");
  const metaRows = await parseCsvBuffer(Buffer.from(metaLines.slice(1).join("\n")));
  const meta = metaRows[0] || {};

  const filmRows = await parseCsvBuffer(Buffer.from(text.slice(filmsIndex)));
  const films = filmRows.map((row) => ({
    position: Number(row.Position) || null,
    title: row.Name || "",
    year: row.Year || "",
  }));

  return { name: (meta.Name || "").trim(), description: meta.Description || "", films };
};

module.exports = {
  parseCsvBuffer,
  getZipEntryBuffer,
  toTopN,
  parseListCsvBuffer,
};
