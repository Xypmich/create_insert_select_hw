CREATE TABLE IF NOT EXISTS singers (
	id SERIAL PRIMARY KEY,
	name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS albums (
	id SERIAL PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	release_year INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tracks (
	id SERIAL PRIMARY KEY,
	name VARCHAR(50) NOT NULL,
	duration NUMERIC NOT NULL,
	album_id INTEGER REFERENCES albums(id)
);

CREATE TABLE IF NOT EXISTS genres (
    id SERIAL PRIMARY KEY,
    name VARCHAR(40) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS collections (
    id SERIAL PRIMARY KEY,
    name VARCHAR(70) NOT NULL,
    release_year INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS singer_genre (
    singer_id INTEGER NOT NULL REFERENCES singers(id),
    genre_id INTEGER NOT NULL REFERENCES genres(id),
    CONSTRAINT sing_gen_pk PRIMARY KEY (singer_id, genre_id)
);

CREATE TABLE IF NOT EXISTS singer_album (
    singer_id INTEGER NOT NULL REFERENCES singers(id),
    album_id INTEGER NOT NULL REFERENCES albums(id),
    CONSTRAINT sing_alb_pk PRIMARY KEY (singer_id, album_id)
);

CREATE TABLE IF NOT EXISTS track_collection (
    track_id INTEGER NOT NULL REFERENCES tracks(id),
    collection_id INTEGER NOT NULL REFERENCES collections(id),
    CONSTRAINT track_collect_pk PRIMARY KEY (track_id, collection_id)
)