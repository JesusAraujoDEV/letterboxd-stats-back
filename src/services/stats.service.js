const AdmZip = require("adm-zip");
const cheerio = require("cheerio");
const { parseCsvBuffer, getZipEntryBuffer, toTopN } = require("../utils/csvHelper");
const { fetchMoviePosterPath, fetchMovieDetailsByTitleYear } = require("../utils/tmdbHelper");

const buildTopDecades = async (ratingsRows) => {
  const decadeMap = {};

  ratingsRows.forEach((row) => {
    const title = row.Name || row.name || row.Title || row["Name"] || row["Title"];
    const yearValue = row.Year || row.year || row["Year"];
    const ratingValue = row.Rating || row.rating || row["Rating"];
    const ratedDate = row.Date || row.date || row["Date"] || null;

    const year = Number(yearValue);
    const rating = parseFloat(ratingValue);

    if (!title || !Number.isFinite(year) || !Number.isFinite(rating)) {
      return;
    }

    const decade = Math.floor(year / 10) * 10;
    const key = String(decade);

    if (!decadeMap[key]) {
      decadeMap[key] = { sum: 0, count: 0, movies: [] };
    }

    decadeMap[key].sum += rating;
    decadeMap[key].count += 1;
    decadeMap[key].movies.push({
      title: String(title).trim(),
      year: String(year),
      userRating: rating,
      ratedDate: ratedDate ? String(ratedDate).trim() : null,
    });
  });

  const decadeAverages = Object.entries(decadeMap)
    .map(([decade, data]) => ({
      decade: Number(decade),
      average: data.count > 0 ? Number((data.sum / data.count).toFixed(2)) : 0,
      movies: data.movies,
    }))
    .sort((a, b) => b.average - a.average)
    .slice(0, 3);

  const sortMovies = (a, b) => {
    if (b.userRating !== a.userRating) return b.userRating - a.userRating;
    if (a.ratedDate && b.ratedDate && a.ratedDate !== b.ratedDate) {
      return b.ratedDate.localeCompare(a.ratedDate);
    }
    return (a.title || "").localeCompare(b.title || "");
  };

  const enrichWithPosterPaths = async (movies) => {
    const batchSize = 5;
    const enriched = movies.map((movie) => ({ ...movie, posterPath: null }));

    for (let i = 0; i < enriched.length; i += batchSize) {
      const batch = enriched.slice(i, i + batchSize);
      const posterPaths = await Promise.all(
        batch.map((movie) => fetchMoviePosterPath(movie.title, movie.year)),
      );

      posterPaths.forEach((posterPath, index) => {
        batch[index].posterPath = posterPath || null;
      });
    }

    return enriched;
  };

  const topDecades = [];
  for (const entry of decadeAverages) {
    const topMovies = entry.movies.sort(sortMovies).slice(0, 8);
    const topMoviesWithPosters = await enrichWithPosterPaths(topMovies);
    topDecades.push({
      decade: `${entry.decade}s`,
      average: entry.average,
      topMovies: topMoviesWithPosters,
    });
  }

  return topDecades;
};

const daysOfWeek = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"];
const monthsOfYear = [
  "Enero",
  "Febrero",
  "Marzo",
  "Abril",
  "Mayo",
  "Junio",
  "Julio",
  "Agosto",
  "Septiembre",
  "Octubre",
  "Noviembre",
  "Diciembre",
];

const getISOWeekNumber = (date) => {
  const utcDate = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNum = utcDate.getUTCDay() || 7;
  utcDate.setUTCDate(utcDate.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(utcDate.getUTCFullYear(), 0, 1));
  const diffDays = Math.floor((utcDate - yearStart) / 86400000) + 1;
  return Math.ceil(diffDays / 7);
};

const parseWatchedDate = (value) => {
  if (!value) return null;
  const dateString = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateString)) return null;
  const parsedDate = new Date(`${dateString}T00:00:00Z`);
  if (Number.isNaN(parsedDate.getTime())) return null;

  const dayIndex = (parsedDate.getUTCDay() + 6) % 7;
  const monthIndex = parsedDate.getUTCMonth();
  return {
    dateString,
    year: dateString.substring(0, 4),
    watchedDay: daysOfWeek[dayIndex],
    watchedWeek: getISOWeekNumber(parsedDate),
    watchedMonth: monthsOfYear[monthIndex],
    watchedMonthIndex: monthIndex,
  };
};

const calculateLongestStreak = (logs) => {
  const dates = [
    ...new Set(
      (logs || [])
        .map((log) => log.watchedDate || log.Date)
        .filter((date) => date != null && date !== "")
        .map((date) => String(date).trim()),
    ),
  ].sort();

  if (dates.length === 0) return 0;

  let longestStreak = 1;
  let currentStreak = 1;

  for (let i = 1; i < dates.length; i += 1) {
    const prevDate = new Date(dates[i - 1]);
    const currDate = new Date(dates[i]);

    const diffTime = Math.abs(currDate - prevDate);
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    if (diffDays === 1) {
      currentStreak += 1;
      if (currentStreak > longestStreak) {
        longestStreak = currentStreak;
      }
    } else if (diffDays > 1) {
      currentStreak = 1;
    }
  }

  return longestStreak;
};

const languageCodeMap = {
  ab: "Abkhazian",
  aa: "Afar",
  af: "Afrikaans",
  ak: "Akan",
  sq: "Albanian",
  am: "Amharic",
  ar: "Arabic",
  an: "Aragonese",
  hy: "Armenian",
  as: "Assamese",
  av: "Avaric",
  ae: "Avestan",
  ay: "Aymara",
  az: "Azerbaijani",
  bm: "Bambara",
  ba: "Bashkir",
  eu: "Basque",
  be: "Belarusian",
  bn: "Bengali",
  bi: "Bislama",
  bs: "Bosnian",
  br: "Breton",
  bg: "Bulgarian",
  my: "Burmese",
  ca: "Catalan",
  ch: "Chamorro",
  ce: "Chechen",
  ny: "Chichewa",
  zh: "Chinese",
  cn: "Cantonese",
  cu: "Church Slavonic",
  cv: "Chuvash",
  kw: "Cornish",
  co: "Corsican",
  cr: "Cree",
  hr: "Croatian",
  cs: "Czech",
  da: "Danish",
  dv: "Divehi",
  nl: "Dutch",
  dz: "Dzongkha",
  en: "English",
  eo: "Esperanto",
  et: "Estonian",
  ee: "Ewe",
  fo: "Faroese",
  fj: "Fijian",
  fi: "Finnish",
  fr: "French",
  fy: "Western Frisian",
  ff: "Fulah",
  gd: "Gaelic",
  gl: "Galician",
  lg: "Ganda",
  ka: "Georgian",
  de: "German",
  el: "Greek",
  kl: "Kalaallisut",
  gn: "Guarani",
  gu: "Gujarati",
  ht: "Haitian",
  ha: "Hausa",
  haw: "Hawaiian",
  he: "Hebrew",
  hz: "Herero",
  hi: "Hindi",
  ho: "Hiri Motu",
  hu: "Hungarian",
  is: "Icelandic",
  io: "Ido",
  ig: "Igbo",
  id: "Indonesian",
  ia: "Interlingua",
  ie: "Interlingue",
  iu: "Inuktitut",
  ik: "Inupiaq",
  ga: "Irish",
  it: "Italian",
  ja: "Japanese",
  jv: "Javanese",
  kn: "Kannada",
  kr: "Kanuri",
  ks: "Kashmiri",
  kk: "Kazakh",
  km: "Central Khmer",
  ki: "Kikuyu",
  rw: "Kinyarwanda",
  ky: "Kyrgyz",
  kv: "Komi",
  kg: "Kongo",
  ko: "Korean",
  kj: "Kuanyama",
  ku: "Kurdish",
  lo: "Lao",
  la: "Latin",
  lv: "Latvian",
  li: "Limburgan",
  ln: "Lingala",
  lt: "Lithuanian",
  lu: "Luba-Katanga",
  lb: "Luxembourgish",
  mk: "Macedonian",
  mg: "Malagasy",
  ms: "Malay",
  ml: "Malayalam",
  mt: "Maltese",
  gv: "Manx",
  mi: "Maori",
  mr: "Marathi",
  mh: "Marshallese",
  mn: "Mongolian",
  na: "Nauru",
  nv: "Navajo",
  nd: "North Ndebele",
  nr: "South Ndebele",
  ng: "Ndonga",
  ne: "Nepali",
  no: "Norwegian",
  nb: "Norwegian Bokmål",
  nn: "Norwegian Nynorsk",
  oc: "Occitan",
  oj: "Ojibwa",
  or: "Oriya",
  om: "Oromo",
  os: "Ossetian",
  pi: "Pali",
  ps: "Pashto",
  fa: "Persian",
  pl: "Polish",
  pt: "Portuguese",
  pa: "Punjabi",
  qu: "Quechua",
  ro: "Romanian",
  rm: "Romansh",
  rn: "Rundi",
  ru: "Russian",
  se: "Northern Sami",
  sm: "Samoan",
  sg: "Sango",
  sa: "Sanskrit",
  sc: "Sardinian",
  sr: "Serbian",
  sn: "Shona",
  sd: "Sindhi",
  si: "Sinhala",
  sk: "Slovak",
  sl: "Slovenian",
  so: "Somali",
  st: "Southern Sotho",
  es: "Spanish",
  su: "Sundanese",
  sw: "Swahili",
  ss: "Swati",
  sv: "Swedish",
  tl: "Tagalog",
  ty: "Tahitian",
  tg: "Tajik",
  ta: "Tamil",
  tt: "Tatar",
  te: "Telugu",
  th: "Thai",
  bo: "Tibetan",
  ti: "Tigrinya",
  to: "Tonga",
  ts: "Tsonga",
  tn: "Tswana",
  tr: "Turkish",
  tk: "Turkmen",
  tw: "Twi",
  ug: "Uighur",
  uk: "Ukrainian",
  ur: "Urdu",
  uz: "Uzbek",
  ve: "Venda",
  vi: "Vietnamese",
  vo: "Volapük",
  wa: "Walloon",
  cy: "Welsh",
  wo: "Wolof",
  xh: "Xhosa",
  ii: "Sichuan Yi",
  yi: "Yiddish",
  yo: "Yoruba",
  za: "Zhuang",
  zu: "Zulu",
};

const countryCodeMap = {
  AF: "Afghanistan",
  AX: "Åland Islands",
  AL: "Albania",
  DZ: "Algeria",
  AS: "American Samoa",
  AD: "Andorra",
  AO: "Angola",
  AI: "Anguilla",
  AQ: "Antarctica",
  AG: "Antigua and Barbuda",
  AR: "Argentina",
  AM: "Armenia",
  AW: "Aruba",
  AU: "Australia",
  AT: "Austria",
  AZ: "Azerbaijan",
  BS: "Bahamas",
  BH: "Bahrain",
  BD: "Bangladesh",
  BB: "Barbados",
  BY: "Belarus",
  BE: "Belgium",
  BZ: "Belize",
  BJ: "Benin",
  BM: "Bermuda",
  BT: "Bhutan",
  BO: "Bolivia",
  BQ: "Bonaire, Sint Eustatius and Saba",
  BA: "Bosnia and Herzegovina",
  BW: "Botswana",
  BV: "Bouvet Island",
  BR: "Brazil",
  IO: "British Indian Ocean Territory",
  VG: "British Virgin Islands",
  BN: "Brunei",
  BG: "Bulgaria",
  BF: "Burkina Faso",
  BI: "Burundi",
  CV: "Cape Verde",
  KH: "Cambodia",
  CM: "Cameroon",
  CA: "Canada",
  KY: "Cayman Islands",
  CF: "Central African Republic",
  TD: "Chad",
  CL: "Chile",
  CN: "China",
  CX: "Christmas Island",
  CC: "Cocos (Keeling) Islands",
  CO: "Colombia",
  KM: "Comoros",
  CK: "Cook Islands",
  CR: "Costa Rica",
  HR: "Croatia",
  CU: "Cuba",
  CW: "Curaçao",
  CY: "Cyprus",
  CZ: "Czechia",
  CD: "Democratic Republic of the Congo",
  DK: "Denmark",
  DJ: "Djibouti",
  DM: "Dominica",
  DO: "Dominican Republic",
  EC: "Ecuador",
  EG: "Egypt",
  SV: "El Salvador",
  GQ: "Equatorial Guinea",
  ER: "Eritrea",
  EE: "Estonia",
  SZ: "Eswatini",
  ET: "Ethiopia",
  FK: "Falkland Islands",
  FO: "Faroe Islands",
  FJ: "Fiji",
  FI: "Finland",
  FR: "France",
  GF: "French Guiana",
  PF: "French Polynesia",
  TF: "French Southern Territories",
  GA: "Gabon",
  GM: "Gambia",
  GE: "Georgia",
  DE: "Germany",
  GH: "Ghana",
  GI: "Gibraltar",
  GR: "Greece",
  GL: "Greenland",
  GD: "Grenada",
  GP: "Guadeloupe",
  GU: "Guam",
  GT: "Guatemala",
  GG: "Guernsey",
  GN: "Guinea",
  GW: "Guinea-Bissau",
  GY: "Guyana",
  HT: "Haiti",
  HM: "Heard Island and McDonald Islands",
  HN: "Honduras",
  HK: "Hong Kong",
  HU: "Hungary",
  IS: "Iceland",
  IN: "India",
  ID: "Indonesia",
  IR: "Iran",
  IQ: "Iraq",
  IE: "Ireland",
  IM: "Isle of Man",
  IL: "Israel",
  IT: "Italy",
  CI: "Ivory Coast",
  JM: "Jamaica",
  JP: "Japan",
  JE: "Jersey",
  JO: "Jordan",
  KZ: "Kazakhstan",
  KE: "Kenya",
  KI: "Kiribati",
  XK: "Kosovo",
  KW: "Kuwait",
  KG: "Kyrgyzstan",
  LA: "Laos",
  LV: "Latvia",
  LB: "Lebanon",
  LS: "Lesotho",
  LR: "Liberia",
  LY: "Libya",
  LI: "Liechtenstein",
  LT: "Lithuania",
  LU: "Luxembourg",
  MO: "Macau",
  MG: "Madagascar",
  MW: "Malawi",
  MY: "Malaysia",
  MV: "Maldives",
  ML: "Mali",
  MT: "Malta",
  MH: "Marshall Islands",
  MQ: "Martinique",
  MR: "Mauritania",
  MU: "Mauritius",
  YT: "Mayotte",
  MX: "Mexico",
  FM: "Micronesia",
  MD: "Moldova",
  MC: "Monaco",
  MN: "Mongolia",
  ME: "Montenegro",
  MS: "Montserrat",
  MA: "Morocco",
  MZ: "Mozambique",
  MM: "Myanmar",
  NA: "Namibia",
  NR: "Nauru",
  NP: "Nepal",
  AN: "Netherlands Antilles",
  NC: "New Caledonia",
  NZ: "New Zealand",
  NI: "Nicaragua",
  NE: "Niger",
  NG: "Nigeria",
  NU: "Niue",
  NF: "Norfolk Island",
  KP: "North Korea",
  MK: "North Macedonia",
  MP: "Northern Mariana Islands",
  NO: "Norway",
  OM: "Oman",
  PK: "Pakistan",
  PW: "Palau",
  PS: "Palestinian Territories",
  PA: "Panama",
  PG: "Papua New Guinea",
  PY: "Paraguay",
  PE: "Peru",
  PH: "Philippines",
  PN: "Pitcairn Islands",
  PL: "Poland",
  PT: "Portugal",
  PR: "Puerto Rico",
  QA: "Qatar",
  CG: "Republic of the Congo",
  RE: "Réunion",
  RO: "Romania",
  RU: "Russia",
  RW: "Rwanda",
  BL: "Saint Barthélemy",
  SH: "Saint Helena",
  KN: "Saint Kitts and Nevis",
  LC: "Saint Lucia",
  MF: "Saint Martin",
  PM: "Saint Pierre and Miquelon",
  VC: "Saint Vincent and the Grenadines",
  WS: "Samoa",
  SM: "San Marino",
  ST: "Sao Tome and Principe",
  SA: "Saudi Arabia",
  SN: "Senegal",
  RS: "Serbia",
  CS: "Serbia and Montenegro",
  SC: "Seychelles",
  SL: "Sierra Leone",
  SG: "Singapore",
  SX: "Sint Maarten",
  SK: "Slovakia",
  SI: "Slovenia",
  SB: "Solomon Islands",
  SO: "Somalia",
  ZA: "South Africa",
  GS: "South Georgia and the South Sandwich Islands",
  KR: "South Korea",
  SS: "South Sudan",
  ES: "Spain",
  LK: "Sri Lanka",
  SD: "Sudan",
  SR: "Suriname",
  SJ: "Svalbard and Jan Mayen",
  SE: "Sweden",
  CH: "Switzerland",
  SY: "Syria",
  TW: "Taiwan",
  TJ: "Tajikistan",
  TZ: "Tanzania",
  TH: "Thailand",
  NL: "The Netherlands",
  TL: "Timor-Leste",
  TG: "Togo",
  TK: "Tokelau",
  TO: "Tonga",
  TT: "Trinidad and Tobago",
  TN: "Tunisia",
  TR: "Turkey",
  TM: "Turkmenistan",
  TC: "Turks and Caicos Islands",
  TV: "Tuvalu",
  VI: "U.S. Virgin Islands",
  UG: "Uganda",
  UA: "Ukraine",
  AE: "United Arab Emirates",
  GB: "United Kingdom",
  US: "United States of America",
  UM: "United States Minor Outlying Islands",
  UY: "Uruguay",
  UZ: "Uzbekistan",
  VU: "Vanuatu",
  VA: "Vatican City",
  VE: "Venezuela",
  VN: "Vietnam",
  WF: "Wallis and Futuna",
  EH: "Western Sahara",
  YE: "Yemen",
  ZM: "Zambia",
  ZW: "Zimbabwe"
};

const buildCacheKey = (title, year) => {
  const safeTitle = title ? String(title).trim().toLowerCase() : "";
  const safeYear = year ? String(year).trim() : "";
  return `${safeTitle}::${safeYear}`;
};

const resolveLetterboxdLink = async (shortUrl) => {
  try {
    const response = await fetch(shortUrl, { method: "HEAD", redirect: "follow" });
    const finalUrl = response.url;
    const finalUrlObj = new URL(finalUrl);
    const pathParts = finalUrlObj.pathname.split("/").filter(Boolean);

    const username = pathParts[0] || "unknown";
    const slug = pathParts[2] || "";

    let itemName = "";
    if (slug) {
      itemName = slug
        .split("-")
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(" ");
    }

    return { username, itemName, finalUrl };
  } catch (error) {
    console.error(`Error resolviendo ${shortUrl}:`, error.message);
    return { username: "unknown", itemName: "", finalUrl: "" };
  }
};

const getUserAvatar = async (username) => {
  try {
    const response = await fetch(`https://letterboxd.com/${username}/`);
    if (!response.ok) return null;

    const html = await response.text();
    const $ = cheerio.load(html);

    let avatarUrl = $("meta[property='og:image']").attr("content");

    if (!avatarUrl || avatarUrl.includes("default-avatar")) {
      avatarUrl = $(".profile-avatar img").attr("src") || $(".avatar img").attr("src");
    }

    return avatarUrl || null;
  } catch (error) {
    console.error(`Error obteniendo avatar para ${username}:`, error.message);
    return null;
  }
};

const incrementPersonCounter = (counter, person, movieTitle) => {
  if (!person || !person.name) return;
  const name = String(person.name).trim();
  const safeTitle = movieTitle ? String(movieTitle).trim() : "";
  if (!name || !safeTitle) return;

  if (!counter[name]) {
    counter[name] = { name, titles: new Set([safeTitle]), profilePath: person.profile_path || null };
  } else {
    counter[name].titles.add(safeTitle);
    if (!counter[name].profilePath && person.profile_path) {
      counter[name].profilePath = person.profile_path;
    }
  }
};

const buildTopMetadataFromWatched = async (watchedRows, diaryRows, likedTitlesSet, detailsCache) => {
  const uniqueMovies = new Map();
  const likedTitlesArray = Array.from(likedTitlesSet || []);

  watchedRows.forEach((row) => {
    const title = row.Name || row.name || row.Title || row["Name"] || row["Title"];
    if (!title) return;

    const yearValue = row.Year || row.year || row["Year"] || row["Year Released"] || row["Release Year"];
    const year = Number(yearValue);
    const normalizedYear = Number.isFinite(year) ? String(year) : "";
    const key = `${String(title).trim().toLowerCase()}::${normalizedYear}`;

    if (!uniqueMovies.has(key)) {
      uniqueMovies.set(key, { title: String(title).trim(), year: normalizedYear || null });
    }
  });

  const movies = Array.from(uniqueMovies.values());
  const diaryByTitle = {};
  diaryRows.forEach((row) => {
    const title = row.Name || row.name || row.Title || row["Name"] || row["Title"];
    if (!title) return;
    const key = String(title).trim();
    if (!key) return;
    if (!diaryByTitle[key]) {
      diaryByTitle[key] = [];
    }
    diaryByTitle[key].push(row);
  });
  const genreCounter = {};
  const countryCounter = {};
  const languageCounter = {};
  const allMovies = [];
  const batchSize = 25;
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  for (let i = 0; i < movies.length; i += batchSize) {
    const batch = movies.slice(i, i + batchSize);
    const detailsList = await Promise.all(
      batch.map(async (movie) => {
        const cacheKey = buildCacheKey(movie.title, movie.year);
        if (detailsCache && detailsCache[cacheKey]) {
          return detailsCache[cacheKey];
        }
        const details = await fetchMovieDetailsByTitleYear(movie.title, movie.year);
        if (detailsCache && details) {
          detailsCache[cacheKey] = details;
        }
        return details;
      }),
    );

    detailsList.forEach((details, index) => {
      const movie = batch[index];
      if (!details) return;

      const movieTitle = movie && movie.title ? String(movie.title).trim() : "";
      const movieYearValue = movie && movie.year ? Number(movie.year) : NaN;
      const decade = Number.isFinite(movieYearValue)
        ? `${Math.floor(movieYearValue / 10) * 10}s`
        : null;
      const diaryLogs = movieTitle ? diaryByTitle[movieTitle] || [] : [];

      const languageCode = details.original_language ? String(details.original_language).trim() : "";
      const language = languageCodeMap[languageCode] || languageCode || null;
      const originCountryCode = Array.isArray(details.origin_country)
        ? String(details.origin_country[0] || "").trim()
        : "";
      const country = countryCodeMap[originCountryCode] || originCountryCode || null;

      const movieObj = {
        title: movieTitle,
        releaseYear: Number.isFinite(movieYearValue) ? movieYearValue : null,
        decade,
        posterPath: details.poster_path || null,
        liked: movieTitle ? likedTitlesArray.includes(movieTitle) : false,
        genres: Array.isArray(details.genres) ? details.genres.map((g) => g.name).filter(Boolean) : [],
        country,
        language,
        directors: Array.isArray(details.credits && details.credits.crew)
          ? details.credits.crew
              .filter((c) => c && c.job === "Director" && c.name)
              .map((d) => d.name)
          : [],
        cast: Array.isArray(details.credits && details.credits.cast)
          ? details.credits.cast
              .slice(0, 10)
              .filter((c) => c && c.name)
              .map((c) => c.name)
          : [],
        diaryLogs: diaryLogs.map((entry) => {
          const watchedMeta = parseWatchedDate(entry["Watched Date"]);
          return {
            rating: entry.Rating ? parseFloat(entry.Rating) : null,
            watchedDate: entry["Watched Date"],
            watchedYear: watchedMeta ? watchedMeta.year : null,
            watchedDay: watchedMeta ? watchedMeta.watchedDay : null,
            watchedWeek: watchedMeta ? watchedMeta.watchedWeek : null,
            watchedMonth: watchedMeta ? watchedMeta.watchedMonth : null,
            tags: entry.Tags ? String(entry.Tags).split(",").map((t) => t.trim()) : [],
          };
        }),
        rewatchCount: diaryLogs.length,
      };

      allMovies.push(movieObj);

      const genres = Array.isArray(details.genres) ? details.genres : [];
      const credits = details.credits || {};

      genres.forEach((genre) => {
        const name = genre && genre.name ? String(genre.name).trim() : "";
        if (name) genreCounter[name] = (genreCounter[name] || 0) + 1;
      });

      if (country) {
        const name = String(country).trim();
        if (name) countryCounter[name] = (countryCounter[name] || 0) + 1;
      }

      if (language) {
        const name = String(language).trim();
        if (name) languageCounter[name] = (languageCounter[name] || 0) + 1;
      }

    });

    if (i + batchSize < movies.length) {
      await delay(200);
    }
  }

  const allCountries = Object.entries(countryCounter)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ name, count }));

  const actorsAllTime = {};
  const directorsAllTime = {};
  const actorsLogged = {};
  const directorsLogged = {};

  const incrementByMovie = (counter, name, profilePath) => {
    const safeName = name ? String(name).trim() : "";
    if (!safeName) return;

    if (!counter[safeName]) {
      counter[safeName] = { name: safeName, count: 1, profilePath: profilePath || null };
      return;
    }

    counter[safeName].count += 1;
    if (!counter[safeName].profilePath && profilePath) {
      counter[safeName].profilePath = profilePath;
    }
  };

  allMovies.forEach((movie) => {
    const movieTitle = movie && movie.title ? String(movie.title).trim() : "";
    const movieYear = movie && Number.isFinite(movie.releaseYear) ? String(movie.releaseYear) : "";
    const cacheKey = movieTitle ? buildCacheKey(movieTitle, movieYear) : "";
    const cachedDetails = cacheKey && detailsCache ? detailsCache[cacheKey] : null;
    const cachedCredits = cachedDetails && cachedDetails.credits ? cachedDetails.credits : {};
    const cachedCast = Array.isArray(cachedCredits.cast) ? cachedCredits.cast : [];
    const cachedCrew = Array.isArray(cachedCredits.crew) ? cachedCredits.crew : [];

    const castProfileByName = new Map(
      cachedCast
        .filter((member) => member && member.name)
        .map((member) => [member.name, member.profile_path || null]),
    );
    const directorProfileByName = new Map(
      cachedCrew
        .filter((member) => member && member.job === "Director" && member.name)
        .map((member) => [member.name, member.profile_path || null]),
    );

    const uniqueCast = new Set(Array.isArray(movie.cast) ? movie.cast.filter(Boolean) : []);
    const uniqueDirectors = new Set(
      Array.isArray(movie.directors) ? movie.directors.filter(Boolean) : [],
    );

    uniqueCast.forEach((name) => {
      incrementByMovie(actorsAllTime, name, castProfileByName.get(name));
    });
    uniqueDirectors.forEach((name) => {
      incrementByMovie(directorsAllTime, name, directorProfileByName.get(name));
    });

    if (movie && Array.isArray(movie.diaryLogs) && movie.diaryLogs.length > 0) {
      uniqueCast.forEach((name) => {
        incrementByMovie(actorsLogged, name, castProfileByName.get(name));
      });
      uniqueDirectors.forEach((name) => {
        incrementByMovie(directorsLogged, name, directorProfileByName.get(name));
      });
    }
  });

  const topActorsAllTime = Object.values(actorsAllTime)
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  const topDirectorsAllTime = Object.values(directorsAllTime)
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  const topActorsLogged = Object.values(actorsLogged)
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  const topDirectorsLogged = Object.values(directorsLogged)
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  return {
    topGenres: toTopN(genreCounter, 10, "name"),
    topCountries: toTopN(countryCounter, 10, "name"),
    topLanguages: toTopN(languageCounter, 10, "name"),
    allCountries,
    topActorsAllTime,
    topActorsLogged,
    topDirectorsAllTime,
    topDirectorsLogged,
    allMovies,
  };
};

const buildTopCreditsFromDiary = async (diaryRows, detailsCache) => {
  const actorsLogged = {};
  const directorsLogged = {};

  for (const row of diaryRows) {
    const title = row.Name || row.name || row.Title || row["Name"] || row["Title"];
    if (!title) continue;

    const yearValue = row.Year || row.year || row["Year"];
    const year = Number(yearValue);
    const yearKey = Number.isFinite(year) ? String(year) : "";
    const cacheKey = buildCacheKey(title, yearKey);

    let details = detailsCache && detailsCache[cacheKey];
    if (!details) {
      details = await fetchMovieDetailsByTitleYear(title, yearKey || null);
      if (detailsCache && details) {
        detailsCache[cacheKey] = details;
      }
    }

    if (!details || !details.credits) continue;

    const cast = Array.isArray(details.credits.cast) ? details.credits.cast : [];
    const crew = Array.isArray(details.credits.crew) ? details.credits.crew : [];

    const safeTitle = String(title).trim();

    crew
      .filter((member) => member && member.job === "Director")
      .forEach((member) => incrementPersonCounter(directorsLogged, member, safeTitle));

    cast
      .slice(0, 5)
      .forEach((member) => incrementPersonCounter(actorsLogged, member, safeTitle));
  }

  return {
    topActorsLogged: Object.values(actorsLogged)
      .map((entry) => ({
        name: entry.name,
        count: entry.titles.size,
        profilePath: entry.profilePath || null,
      }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10),
    topDirectorsLogged: Object.values(directorsLogged)
      .map((entry) => ({
        name: entry.name,
        count: entry.titles.size,
        profilePath: entry.profilePath || null,
      }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10),
  };
};

const buildTotalHoursWatched = async (diaryRows, detailsCache) => {
  const getCacheKey = (title, year) => {
    const safeTitle = title ? String(title).trim().toLowerCase() : "";
    const safeYear = year ? String(year).trim() : "";
    return `${safeTitle}::${safeYear}`;
  };

  let totalMinutes = 0;

  for (const row of diaryRows) {
    const title = row.Name || row.name || row.Title || row["Name"] || row["Title"];
    if (!title) continue;

    const yearValue = row.Year || row.year || row["Year"];
    const year = Number(yearValue);
    const yearKey = Number.isFinite(year) ? String(year) : "";
    const cacheKey = getCacheKey(title, yearKey);

    let details = detailsCache && detailsCache[cacheKey];
    if (!details) {
      details = await fetchMovieDetailsByTitleYear(title, yearKey || null);
      if (detailsCache && details) {
        detailsCache[cacheKey] = details;
      }
    }

    const runtime = details && Number.isFinite(details.runtime) ? details.runtime : 0;
    if (runtime > 0) {
      totalMinutes += runtime;
    }
  }

  return Math.round(totalMinutes / 60);
};

const buildStatsFromZipBuffer = async (zipBuffer) => {
  let zip;
  try {
    zip = new AdmZip(zipBuffer);
  } catch (err) {
    throw new Error("El archivo no es un ZIP válido.");
  }

  const watchedBuffer = getZipEntryBuffer(zip, "watched.csv");
  const ratingsBuffer = getZipEntryBuffer(zip, "ratings.csv");
  const diaryBuffer = getZipEntryBuffer(zip, "diary.csv");

  const safeParseCsv = async (filename) => {
    try {
      const buffer = getZipEntryBuffer(zip, filename);
      return await parseCsvBuffer(buffer);
    } catch (err) {
      return [];
    }
  };

  const [
    watchedRows,
    ratingsRows,
    diaryRows,
    profileRows,
    watchlistRows,
    reviewsRows,
    commentsRows,
    deletedDiaryRows,
    deletedReviewsRows,
    deletedCommentsRows,
    likedFilmsRows,
    likedListsRows,
    likedReviewsRows,
  ] = await Promise.all([
    parseCsvBuffer(watchedBuffer),
    parseCsvBuffer(ratingsBuffer),
    parseCsvBuffer(diaryBuffer),
    safeParseCsv("profile.csv"),
    safeParseCsv("watchlist.csv"),
    safeParseCsv("reviews.csv"),
    safeParseCsv("comments.csv"),
    safeParseCsv("deleted/diary.csv"),
    safeParseCsv("deleted/reviews.csv"),
    safeParseCsv("deleted/comments.csv"),
    safeParseCsv("likes/films.csv"),
    safeParseCsv("likes/lists.csv"),
    safeParseCsv("likes/reviews.csv"),
  ]);

  const profileRow = profileRows[0] || {};
  const profile = {
    username: profileRow.Username || profileRow.username || "",
    location: profileRow.Location || profileRow.location || "",
    bio: profileRow.Bio || profileRow.bio || "",
  };
  const normalizedMainUsername = profile.username ? String(profile.username).trim().toLowerCase() : "";

  const totalMovies = watchedRows.length;
  const totalLoggedMovies = diaryRows.length;

  const activityByYear = {};
  const availableYearsSet = new Set();

  const initActivityYear = (year) => {
    if (activityByYear[year]) return;
    activityByYear[year] = {
      days: daysOfWeek.map((day) => ({ day, count: 0 })),
      weeks: Array.from({ length: 52 }, (_, index) => ({ week: index + 1, count: 0 })),
      months: monthsOfYear.map((month) => ({ month, count: 0 })),
    };
  };

  const activityTotal = {
    days: daysOfWeek.map((day) => ({ day, count: 0 })),
    weeks: Array.from({ length: 52 }, (_, index) => ({ week: index + 1, count: 0 })),
    months: monthsOfYear.map((month) => ({ month, count: 0 })),
  };

  activityByYear.Total = activityTotal;

  diaryRows.forEach((row) => {
    const watchedMeta = parseWatchedDate(
      row["Watched Date"] || row["WatchedDate"] || row.watchedDate || null,
    );
    if (!watchedMeta) return;

    const year = watchedMeta.year;
    initActivityYear(year);
    availableYearsSet.add(year);

    const dayIndex = daysOfWeek.indexOf(watchedMeta.watchedDay);
    if (dayIndex >= 0) {
      activityByYear[year].days[dayIndex].count += 1;
      activityByYear.Total.days[dayIndex].count += 1;
    }

    const weekNumber = watchedMeta.watchedWeek;
    while (activityByYear[year].weeks.length < weekNumber) {
      activityByYear[year].weeks.push({ week: activityByYear[year].weeks.length + 1, count: 0 });
    }
    if (weekNumber >= 1) {
      activityByYear[year].weeks[weekNumber - 1].count += 1;
      while (activityByYear.Total.weeks.length < weekNumber) {
        activityByYear.Total.weeks.push({
          week: activityByYear.Total.weeks.length + 1,
          count: 0,
        });
      }
      activityByYear.Total.weeks[weekNumber - 1].count += 1;
    }

    const monthIndex = Number.isFinite(watchedMeta.watchedMonthIndex)
      ? watchedMeta.watchedMonthIndex
      : -1;
    if (monthIndex >= 0 && monthIndex < activityByYear[year].months.length) {
      activityByYear[year].months[monthIndex].count += 1;
      activityByYear.Total.months[monthIndex].count += 1;
    }
  });

  const availableYears = [
    "Total",
    ...Array.from(availableYearsSet).sort((a, b) => b.localeCompare(a)),
  ];
  const activityStats = {
    availableYears,
    byYear: activityByYear,
  };

  const watchedYearMap = {};

  diaryRows.forEach((row) => {
    const watchedDateValue =
      row["Watched Date"] || row["WatchedDate"] || row.watchedDate || row.date || null;
    if (!watchedDateValue) return;

    const watchedDateString = String(watchedDateValue).trim();
    if (watchedDateString.length < 4) return;

    const year = watchedDateString.substring(0, 4);
    if (!/^[0-9]{4}$/.test(year)) return;

    if (!watchedYearMap[year]) {
      watchedYearMap[year] = { year, count: 0, ratingSum: 0, ratingCount: 0 };
    }

    watchedYearMap[year].count += 1;

    const ratingValue = row.Rating || row.rating || row["Rating"] || null;
    const rating = parseFloat(ratingValue);
    if (Number.isFinite(rating)) {
      watchedYearMap[year].ratingSum += rating;
      watchedYearMap[year].ratingCount += 1;
    }
  });

  const watchedYearStats = Object.values(watchedYearMap)
    .map((entry) => ({
      year: entry.year,
      count: entry.count,
      averageRating:
        entry.ratingCount > 0 ? Number((entry.ratingSum / entry.ratingCount).toFixed(2)) : 0,
    }))
    .sort((a, b) => a.year.localeCompare(b.year));

  const longestStreak = calculateLongestStreak(
    diaryRows.map((row) => ({
      watchedDate: row["Watched Date"] || row["WatchedDate"] || row.watchedDate || row.Date,
    })),
  );

  let ratingSum = 0;
  let ratingCount = 0;
  const ratingDistribution = {};

  ratingsRows.forEach((row) => {
    const raw = row.Rating || row.rating || row["Rating"];
    const rating = parseFloat(raw);
    if (Number.isFinite(rating)) {
      ratingSum += rating;
      ratingCount += 1;
      const key = rating.toString();
      ratingDistribution[key] = (ratingDistribution[key] || 0) + 1;
    }
  });

  const averageRating = ratingCount > 0 ? Number((ratingSum / ratingCount).toFixed(2)) : 0;

  const ratingYearMap = {};
  ratingsRows.forEach((row) => {
    const yearValue = row.Year || row.year || row["Year"];
    const ratingValue = row.Rating || row.rating || row["Rating"];
    const year = Number(yearValue);
    const rating = parseFloat(ratingValue);

    if (Number.isFinite(year) && Number.isFinite(rating)) {
      const key = String(year);
      if (!ratingYearMap[key]) {
        ratingYearMap[key] = { sum: 0, count: 0 };
      }
      ratingYearMap[key].sum += rating;
      ratingYearMap[key].count += 1;
    }
  });

  const ratingYearKeys = Object.keys(ratingYearMap)
    .map((year) => Number(year))
    .filter((year) => Number.isFinite(year));
  const minRatingYear = ratingYearKeys.length > 0 ? Math.min(...ratingYearKeys) : 0;
  const maxRatingYear = ratingYearKeys.length > 0 ? Math.max(...ratingYearKeys) : 0;
  const averageRatingByReleaseYear = [];

  if (minRatingYear && maxRatingYear) {
    for (let year = minRatingYear; year <= maxRatingYear; year += 1) {
      const entry = ratingYearMap[String(year)];
      const average = entry && entry.count > 0 ? Number((entry.sum / entry.count).toFixed(2)) : 0;
      averageRatingByReleaseYear.push({ year: String(year), average });
    }
  }

  const yearCounter = {};
  watchedRows.forEach((row) => {
    const year = row.Year || row["Year"] || row["Year Released"] || row["Release Year"];
    if (year) {
      const key = String(year).trim();
      if (key) {
        yearCounter[key] = (yearCounter[key] || 0) + 1;
      }
    }
  });

  const topYears = toTopN(yearCounter, 5, "year");
  const yearKeys = Object.keys(yearCounter)
    .map((year) => Number(year))
    .filter((year) => Number.isFinite(year));
  const minYear = yearKeys.length > 0 ? Math.min(...yearKeys) : 0;
  const maxYear = yearKeys.length > 0 ? Math.max(...yearKeys) : 0;
  const moviesByReleaseYear = [];

  if (minYear && maxYear) {
    for (let year = minYear; year <= maxYear; year += 1) {
      const count = yearCounter[String(year)] || 0;
      moviesByReleaseYear.push({ year: String(year), count });
    }
  }

  const tagCounter = {};
  diaryRows.forEach((row) => {
    const tagsValue = row.Tags || row.tags || row["Tags"];
    if (!tagsValue) return;

    const tags = String(tagsValue)
      .split(",")
      .map((tag) => tag.trim())
      .filter(Boolean);

    tags.forEach((tag) => {
      tagCounter[tag] = (tagCounter[tag] || 0) + 1;
    });
  });

  const topTags = toTopN(tagCounter, 5, "tag");

  const rewatchCounts = {};
  diaryRows.forEach((row) => {
    const title = row.Name || row.name || row.Title || row["Name"] || row["Title"];
    if (!title) return;

    const key = String(title).trim();
    if (!key) return;

    if (!rewatchCounts[key]) {
      rewatchCounts[key] = { title: key, count: 1 };
    } else {
      rewatchCounts[key].count += 1;
    }
  });

  const mostRewatchedMoviesBase = Object.values(rewatchCounts)
    .filter((entry) => entry.count > 1)
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);

  const mostRewatchedMovies = await Promise.all(
    mostRewatchedMoviesBase.map(async (movie) => ({
      ...movie,
      posterPath: await fetchMoviePosterPath(movie.title, null),
    })),
  );

  const totalWatchlist = watchlistRows.length;
  const totalReviews = reviewsRows.length;
  const totalComments = commentsRows.length;

  const interactionsMap = new Map();
  for (const row of commentsRows) {
    const shortLink = row.Content || row.content || row["Content"] || null;
    const commentText = row.Comment || row.comment || row["Comment"] || "";
    const date = row.Date || row.date || row["Date"] || null;

    if (!shortLink || !String(shortLink).includes("boxd.it")) {
      continue;
    }

    const { username: resolvedUsername, itemName, finalUrl } = await resolveLetterboxdLink(
      String(shortLink).trim(),
    );
    const normalizedUsername = resolvedUsername ? String(resolvedUsername).trim().toLowerCase() : "";
    if (
      resolvedUsername === "unknown" ||
      (normalizedMainUsername && normalizedUsername === normalizedMainUsername)
    ) {
      continue;
    }

    if (!interactionsMap.has(resolvedUsername)) {
      interactionsMap.set(resolvedUsername, {
        username: resolvedUsername,
        interactionCount: 0,
        comments: [],
      });
    }

    const userStats = interactionsMap.get(resolvedUsername);
    userStats.interactionCount += 1;
    userStats.comments.push({ date, text: commentText, movie: itemName, finalUrl });
  }

  const topInteractedUsers = Array.from(interactionsMap.values()).sort(
    (a, b) => b.interactionCount - a.interactionCount,
  );

  const top15Users = topInteractedUsers.slice(0, 15);
  for (const user of top15Users) {
    user.avatarUrl = await getUserAvatar(user.username);
  }

  const top10Users = topInteractedUsers.slice(0, 10);
  const tmdbPosterCache = new Map();
  for (const user of top10Users) {
    for (const comment of user.comments) {
      if (comment.movie) {
        if (!tmdbPosterCache.has(comment.movie)) {
          const posterPath = await fetchMoviePosterPath(comment.movie, null);
          const fullPosterUrl = posterPath
            ? `https://image.tmdb.org/t/p/w200${posterPath}`
            : null;
          tmdbPosterCache.set(comment.movie, fullPosterUrl);
        }
        comment.posterUrl = tmdbPosterCache.get(comment.movie);
      }
    }
  }

  const deletedDiaryCount = deletedDiaryRows.length;
  const deletedReviewsCount = deletedReviewsRows.length;
  const deletedCommentsCount = deletedCommentsRows.length;

  const deletedListsEntries = zip
    .getEntries()
    .filter(
      (entry) =>
        entry.entryName.toLowerCase().startsWith("deleted/lists/") &&
        entry.entryName.toLowerCase().endsWith(".csv"),
    );
  const deletedListsCount = deletedListsEntries.length;
  const deletedListsNames = deletedListsEntries.map((entry) => {
    const baseName = entry.entryName.split("/").pop() || "";
    return baseName.replace(/\.csv$/i, "").replace(/-/g, " ").trim();
  });

  const totalLikedFilms = likedFilmsRows.length;
  const totalLikedLists = likedListsRows.length;
  const totalLikedReviews = likedReviewsRows.length;

  const likedYearCounter = {};
  const likedTitlesSet = new Set();
  likedFilmsRows.forEach((row) => {
    const likedTitle = row.Name || row.name || row.Title || row["Name"] || row["Title"];
    if (likedTitle) {
      const key = String(likedTitle).trim();
      if (key) {
        likedTitlesSet.add(key);
      }
    }
    const year = row.Year || row.year || row["Year"];
    if (year) {
      const key = String(year).trim();
      if (key) {
        likedYearCounter[key] = (likedYearCounter[key] || 0) + 1;
      }
    }
  });
  const topLikedYears = toTopN(likedYearCounter, 3, "year");

  const tmdbDetailsCache = {};
  const topDecades = await buildTopDecades(ratingsRows);
  const {
    topGenres,
    topCountries,
    topLanguages,
    allCountries,
    topActorsAllTime,
    topActorsLogged,
    topDirectorsAllTime,
    topDirectorsLogged,
    allMovies,
  } = await buildTopMetadataFromWatched(
    watchedRows,
    diaryRows,
    likedTitlesSet,
    tmdbDetailsCache,
  );
  const totalHoursWatched = await buildTotalHoursWatched(diaryRows, tmdbDetailsCache);

  return {
    profile,
    totalMovies,
    totalLoggedMovies,
    averageRating,
    ratingDistribution,
    topYears,
    moviesByReleaseYear,
    averageRatingByReleaseYear,
    topTags,
    mostRewatchedMovies,
    totalWatchlist,
    totalReviews,
    totalComments,
    topInteractedUsers,
    deletedDiaryCount,
    deletedReviewsCount,
    deletedCommentsCount,
    deletedListsCount,
    deletedListsNames,
    totalLikedFilms,
    totalLikedLists,
    totalLikedReviews,
    topLikedYears,
    longestStreak,
    topDecades,
    topGenres,
    topCountries,
    topLanguages,
    allCountries,
    topActorsAllTime,
    topActorsLogged,
    topDirectorsAllTime,
    topDirectorsLogged,
    totalHoursWatched,
    activityStats,
    watchedYearStats,
    allMovies,
  };
};

module.exports = {
  buildStatsFromZipBuffer,
};
