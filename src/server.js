const express = require("express");
const cors = require("cors");
const path = require("path");
const YAML = require("yamljs");
const swaggerUi = require("swagger-ui-express");
const SwaggerParser = require("@apidevtools/swagger-parser");
require("dotenv").config();
const statsRoutes = require("./routes/stats.routes");

const app = express();

const corsWhitelist = (process.env.FRONTEND_URLS || "")
  .split(",")
  .map((url) => url.trim())
  .filter(Boolean);

const corsOptions = {
  origin: (origin, callback) => {
    if (!origin || corsWhitelist.includes(origin)) {
      return callback(null, true);
    }
    return callback(new Error("Origen no permitido por CORS"));
  },
};

app.use(cors(corsOptions));
app.use(express.json());

const swaggerPath = path.join(__dirname, "../swagger/openapi.yaml");
const setupSwagger = async () => {
  const swaggerDocument = await SwaggerParser.bundle(swaggerPath);
  swaggerDocument.servers = [
    {
      url: process.env.BACKEND_URL || `http://localhost:${process.env.PORT || 3000}`,
    },
  ];

  app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerDocument));
};

setupSwagger().catch((err) => {
  console.error("Error inicializando Swagger:", err.message || err);
});

app.use("/api", statsRoutes);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
