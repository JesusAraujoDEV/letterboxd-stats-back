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
  const normalized = filename.toLowerCase();
  const entry = zip
    .getEntries()
    .find((e) => e.entryName.toLowerCase().endsWith(normalized));

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

module.exports = {
  parseCsvBuffer,
  getZipEntryBuffer,
  toTopN,
};
