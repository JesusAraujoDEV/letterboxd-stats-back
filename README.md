# Letterboxd Stats API

<div align="center">

	<h1>🎬 Letterboxd Stats API</h1>

	<p><strong>Motor de analítica para exportaciones de Letterboxd</strong></p>
	<p>API en Node.js + Express que procesa ZIP/CSV, enriquece los datos con TMDB y devuelve estadísticas avanzadas de consumo cinematográfico.</p>

	<p>
		<img src="https://img.shields.io/badge/version-1.0.0-0ea5e9?style=for-the-badge" alt="Versión" />
		<img src="https://img.shields.io/badge/license-ISC-111827?style=for-the-badge" alt="Licencia" />
		<img src="https://img.shields.io/badge/node-%3E=20-339933?style=for-the-badge&logo=node.js&logoColor=white" alt="Node.js" />
		<img src="https://img.shields.io/badge/express-5.2.1-000000?style=for-the-badge&logo=express&logoColor=white" alt="Express" />
		<img src="https://img.shields.io/badge/openapi-3.0.3-6b7280?style=for-the-badge&logo=swagger&logoColor=white" alt="OpenAPI" />
	</p>

</div>

---

## ✨ Características Principales

### Frontend

- En este repositorio **no existe frontend**; la API está pensada para ser consumida por una interfaz externa.
- La documentación interactiva está expuesta en Swagger UI para acelerar la integración.

### Backend

- Procesa archivos ZIP exportados desde Letterboxd con `multipart/form-data`.
- Extrae y analiza CSVs de actividad, valoraciones, comentarios, likes, watchlist y borrados.
- Genera métricas de alto nivel: promedio de rating, distribución, décadas top, años top, rachas y actividad temporal.
- Enriquece resultados con TMDB cuando existe `TMDB_API_KEY` configurada.
- Resuelve interacciones sociales a partir de enlaces cortos de Letterboxd y expone avatares y contexto de usuarios.
- Publica documentación OpenAPI en `/api-docs`.
- Aplica CORS por whitelist y soporta despliegues detrás de proxies o plataformas gestionadas.

## 🏗️ Arquitectura y Tecnologías

| Capa | Tecnología / Servicio | Rol dentro del proyecto |
|---|---|---|
| Frontend | No incluido en este repositorio | Consumidor externo de la API |
| Backend | Node.js 20+, Express 5 | API HTTP y orquestación de análisis |
| Parsing | `multer`, `adm-zip`, `csv-parser`, `cheerio` | Carga de ZIP, lectura de CSV y scraping puntual |
| Documentación | `swagger-ui-express`, `@apidevtools/swagger-parser`, YAML OpenAPI | Portal de contrato y exploración de endpoints |
| Integraciones | TMDB API, Letterboxd | Enriquecimiento de metadatos y resolución de perfiles |
| Infra / Runtime | `cors`, `dotenv`, `nodemon` | Configuración, entorno local y CORS seguro |
| Base de datos | No aplica | El análisis se realiza sobre el ZIP subido |

## 🔌 Documentación de la API

### Resumen de Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/api/upload-stats` | Sube un ZIP exportado desde Letterboxd y devuelve el set completo de estadísticas calculadas. |

### `POST /api/upload-stats`

Sube un ZIP de Letterboxd en el campo `file` y devuelve un objeto JSON con todas las métricas calculadas.

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/api/upload-stats` | Analiza el ZIP, lee `watched.csv`, `ratings.csv`, `diary.csv` y archivos opcionales, y devuelve estadísticas enriquecidas. |

**Payload esperado**

```json
{
	"file": "multipart/form-data -> archivo ZIP de exportación de Letterboxd"
}
```

**Ejemplo de petición**

```bash
curl -X POST "http://localhost:3000/api/upload-stats" \
	-F "file=@./export-letterboxd.zip"
```

**Response 200 OK**

```json
{
	"profile": {
		"username": "johndoe",
		"location": "Madrid, Spain",
		"bio": "Cinéfilo y coleccionista de listas"
	},
	"totalMovies": 232,
	"totalLoggedMovies": 418,
	"averageRating": 3.94,
	"ratingDistribution": {
		"1": 2,
		"2.5": 5,
		"3": 15,
		"4": 80,
		"5": 130
	},
	"topYears": [
		{ "year": "2025", "count": 33 },
		{ "year": "2024", "count": 28 }
	],
	"moviesByReleaseYear": [
		{ "year": "2019", "count": 18 },
		{ "year": "2020", "count": 21 }
	],
	"averageRatingByReleaseYear": [
		{ "year": "2019", "average": 3.87 },
		{ "year": "2020", "average": 4.02 }
	],
	"topTags": [
		{ "tag": "festive", "count": 9 },
		{ "tag": "rewatch", "count": 7 }
	],
	"mostRewatchedMovies": [
		{
			"title": "Perfect Blue",
			"count": 4,
			"posterPath": "/poster.jpg"
		}
	],
	"totalWatchlist": 120,
	"totalReviews": 54,
	"totalComments": 88,
	"topInteractedUsers": [
		{
			"username": "alice",
			"interactionCount": 12,
			"avatarUrl": "https://...",
			"comments": []
		}
	],
	"deletedDiaryCount": 3,
	"deletedReviewsCount": 1,
	"deletedCommentsCount": 2,
	"deletedListsCount": 4,
	"deletedListsNames": ["top-2024", "watchlist-cleanup"],
	"totalLikedFilms": 65,
	"totalLikedLists": 19,
	"totalLikedReviews": 27,
	"topLikedYears": [
		{ "year": "2024", "count": 18 }
	],
	"longestStreak": 11,
	"topDecades": [
		{
			"decade": "1990s",
			"average": 4.28,
			"topMovies": []
		}
	],
	"topGenres": [],
	"topCountries": [],
	"topLanguages": [],
	"allCountries": [],
	"topActorsAllTime": [],
	"topActorsLogged": [],
	"topDirectorsAllTime": [],
	"topDirectorsLogged": [],
	"totalHoursWatched": 512.5,
	"activityStats": {
		"availableYears": ["Total", "2025", "2024"],
		"byYear": {}
	},
	"watchedYearStats": [
		{ "year": "2024", "count": 187, "averageRating": 3.91 }
	],
	"allMovies": []
}
```

**Campos devueltos por el service**

| Campo | Tipo | Descripción |
|---|---|---|
| `profile` | objeto | Perfil principal detectado desde `profile.csv`. |
| `totalMovies` | entero | Total de películas en `watched.csv`. |
| `totalLoggedMovies` | entero | Total de entradas en `diary.csv`. |
| `averageRating` | número | Promedio global de ratings. |
| `ratingDistribution` | objeto | Conteo por valor de rating. |
| `topYears` | array | Años de release con más películas vistas. |
| `moviesByReleaseYear` | array | Distribución completa de películas por año de estreno. |
| `averageRatingByReleaseYear` | array | Promedio de rating por año de estreno. |
| `topTags` | array | Tags más frecuentes en el diario. |
| `mostRewatchedMovies` | array | Películas con más rewatch, con poster de TMDB si aplica. |
| `totalWatchlist` | entero | Número de elementos en watchlist. |
| `totalReviews` | entero | Número de reviews registradas. |
| `totalComments` | entero | Número de comentarios registrados. |
| `topInteractedUsers` | array | Usuarios con más interacciones detectadas por comentarios. |
| `deletedDiaryCount` | entero | Entradas de diario eliminadas. |
| `deletedReviewsCount` | entero | Reviews eliminadas. |
| `deletedCommentsCount` | entero | Comentarios eliminados. |
| `deletedListsCount` | entero | Listas eliminadas. |
| `deletedListsNames` | array | Nombres de las listas eliminadas. |
| `totalLikedFilms` | entero | Películas con like. |
| `totalLikedLists` | entero | Listas con like. |
| `totalLikedReviews` | entero | Reviews con like. |
| `topLikedYears` | array | Años más likeados. |
| `longestStreak` | entero | Mayor racha de días consecutivos logueando películas. |
| `topDecades` | array | Décadas mejor puntuadas, con top de películas por década. |
| `topGenres` | array | Géneros más frecuentes. |
| `topCountries` | array | Países más frecuentes. |
| `topLanguages` | array | Idiomas más frecuentes. |
| `allCountries` | array | Catálogo agregado de países detectados. |
| `topActorsAllTime` | array | Actores top en todo el histórico. |
| `topActorsLogged` | array | Actores top en películas logueadas. |
| `topDirectorsAllTime` | array | Directores top en todo el histórico. |
| `topDirectorsLogged` | array | Directores top en películas logueadas. |
| `totalHoursWatched` | número | Horas totales estimadas de visionado. |
| `activityStats` | objeto | Actividad por día, semana y mes, con vista total y por año. |
| `watchedYearStats` | array | Conteo y rating medio por año de visionado. |
| `allMovies` | array | Dataset enriquecido de películas procesadas. |

**Errores típicos**

| Código | Motivo | Ejemplo |
|---|---|---|
| `400` | No se envió el archivo en `file`. | `{"error":"Archivo requerido en el campo 'file'."}` |
| `400` | El ZIP es inválido o no contiene los CSV requeridos. | `{"error":"El archivo no es un ZIP válido."}` |
| `500` | Error interno durante el procesamiento. | `{"error":"Error interno del servidor."}` |

## 🚀 Guía de Inicio Rápido

### Requisitos previos

- Node.js `>= 20`
- `npm` o `pnpm`
- Opcional: Docker si vas a desplegar en contenedores

### Instalación local

```bash
git clone https://github.com/JesusAraujoDEV/letterboxd-stats-back.git
cd letterboxd-stats-back
npm install
```

Si prefieres `pnpm`:

```bash
pnpm install
```

### Variables de entorno

Este proyecto no incluye `.env.example`, pero estas son las variables que consume el backend:

```dotenv
PORT=3000
BACKEND_URL=http://localhost:3000
FRONTEND_URLS=http://localhost:5173,http://localhost:3001
TMDB_API_KEY=tu_token_bearer_de_tmdb
```

### Levantar en desarrollo

```bash
npm run dev
```

Endpoints útiles en local:

- API principal: `http://localhost:3000/api/upload-stats`
- Swagger UI: `http://localhost:3000/api-docs`

## 🐳 Despliegue y Docker

El backend está preparado para correr como servicio Node.js estándar: usa `npm start`, escucha en `PORT` y no requiere base de datos persistente.

### Ejecución en contenedor

```bash
docker build -t letterboxd-stats-back .
docker run --rm -p 3000:3000 --env-file .env letterboxd-stats-back
```

### Notas para producción

- En plataformas como **Dokploy** o constructores tipo **Nixpacks**, basta con apuntar el build al directorio raíz y usar `npm start` como comando de arranque.
- Asegura que `FRONTEND_URLS` incluya los orígenes reales del cliente web para evitar bloqueos por CORS.
- Si configuras `TMDB_API_KEY`, el backend enriquecerá automáticamente algunas métricas con posters y metadatos extra.

## 📂 Estructura del Proyecto

```text
.
├── package.json
├── package-lock.json
├── README.md
├── src
│   ├── server.js
│   ├── controllers
│   │   └── stats.controller.js
│   ├── routes
│   │   └── stats.routes.js
│   ├── services
│   │   └── stats.service.js
│   └── utils
│       ├── csvHelper.js
│       └── tmdbHelper.js
└── swagger
		├── openapi.yaml
		└── stats.yaml
```

## 📎 Documentación adicional

- Swagger UI: `/api-docs`
- Contrato OpenAPI: [`swagger/openapi.yaml`](swagger/openapi.yaml)
- Esquema específico de estadísticas: [`swagger/stats.yaml`](swagger/stats.yaml)

