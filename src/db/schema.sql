CREATE SCHEMA IF NOT EXISTS letterboxd;

CREATE TABLE IF NOT EXISTS letterboxd.films (
  slug text PRIMARY KEY,
  letterboxd_uri text UNIQUE,
  tmdb_id integer,
  tmdb_type text NOT NULL DEFAULT 'movie' CHECK (tmdb_type IN ('movie', 'tv')),
  title text NOT NULL,
  year integer,
  poster_path text,
  rating numeric(2, 1) CHECK (rating BETWEEN 0.5 AND 5),
  watched boolean NOT NULL DEFAULT false,
  liked boolean NOT NULL DEFAULT false,
  in_watchlist boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS letterboxd.diary_entries (
  slug text NOT NULL REFERENCES letterboxd.films (slug) ON DELETE CASCADE,
  watched_on date NOT NULL,
  rating numeric(2, 1) CHECK (rating BETWEEN 0.5 AND 5),
  rewatch boolean NOT NULL DEFAULT false,
  tags text,
  review text,
  PRIMARY KEY (slug, watched_on)
);

CREATE TABLE IF NOT EXISTS letterboxd.recommendations (
  tmdb_id integer PRIMARY KEY,
  title text NOT NULL,
  year integer,
  poster_path text,
  reason text NOT NULL,
  score numeric NOT NULL,
  netflix_countries text[] NOT NULL DEFAULT '{}',
  netflix_id text
);

ALTER TABLE letterboxd.recommendations
  DROP COLUMN IF EXISTS country,
  DROP COLUMN IF EXISTS providers,
  ADD COLUMN IF NOT EXISTS netflix_countries text[] NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS letterboxd.sync_runs (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source text NOT NULL CHECK (source IN ('feed', 'zip')),
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  ok boolean,
  error text,
  summary jsonb
);

CREATE INDEX IF NOT EXISTS sync_runs_started_idx ON letterboxd.sync_runs (started_at DESC);
