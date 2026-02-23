const express = require("express");
const cors = require("cors");
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

app.use("/api", statsRoutes);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
