const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");

let pool;

const getPool = () => {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL no configurada.");
  }
  pool = pool || new Pool({ connectionString: process.env.DATABASE_URL, max: 5 });
  return pool;
};

const query = (text, params) => getPool().query(text, params);

const withTransaction = async (fn) => {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
};

const ensureSchema = () =>
  query(fs.readFileSync(path.join(__dirname, "../db/schema.sql"), "utf8"));

module.exports = { query, withTransaction, ensureSchema };
