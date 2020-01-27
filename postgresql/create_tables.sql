/* --------- credits */

CREATE TABLE IF NOT EXISTS public.credits
(
	cast_ VARCHAR,
	crew VARCHAR,
	id int
)
WITH (
    OIDS = FALSE
);

ALTER TABLE public.credits
    OWNER to postgres;


COPY public.credits(
	cast_,
	crew,
	id
)
FROM '/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/credits.csv'
DELIMITER ','
CSV HEADER;

/* --------- keywords */
CREATE TABLE IF NOT EXISTS public.keywords
(
	id int,
	keywords VARCHAR
)
WITH (
    OIDS = FALSE
);

ALTER TABLE public.keywords
    OWNER to postgres;


COPY public.keywords(
	id,
	keywords
)
FROM '/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/keywords.csv'
DELIMITER ','
CSV HEADER;

/* --------- links */
CREATE TABLE IF NOT EXISTS public.links
(
	movieId int,
	imdbId int,
	tmdbId int
)
WITH (
    OIDS = FALSE
);

ALTER TABLE public.links
    OWNER to postgres;


COPY public.links(
	movieId,
	imdbId,
	tmdbId
)
FROM '/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/links.csv'
DELIMITER ','
CSV HEADER;
/* --------- links_small */
CREATE TABLE IF NOT EXISTS public.links_small
(
	movieId int,
	imdbId int,
	tmdbId int
)
WITH (
    OIDS = FALSE
);

ALTER TABLE public.links_small
    OWNER to postgres;


COPY public.links_small(
	movieId,
	imdbId,
	tmdbId
)
FROM '/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/links_small.csv'
DELIMITER ','
CSV HEADER;
/* --------- movies_metadata */
CREATE TABLE IF NOT EXISTS public.movies_metadata
(
	adult BOOLEAN,
	belongs_to_collection VARCHAR,
	budget INT,
	genres VARCHAR,
	homepage VARCHAR,
	id INT,
	imdb_id VARCHAR,
	original_language CHAR(2),
	original_title VARCHAR(256),
	overview VARCHAR,
	popularity FLOAT,
	poster_path VARCHAR(128),
	production_companies VARCHAR,
	production_countries VARCHAR,
	release_date DATE,
	revenue BIGINT,
	runtime FLOAT,
	spoken_languages VARCHAR,
	status VARCHAR,
	tagline VARCHAR,
	title VARCHAR,
	video BOOLEAN,
	vote_average FLOAT,
	vote_count INT
)
WITH (
    OIDS = FALSE
);

ALTER TABLE public.movies_metadata
    OWNER to postgres;


COPY public.movies_metadata(
	adult,
	belongs_to_collection,
	budget,
	genres,
	homepage,
	id,
	imdb_id,
	original_language,
	original_title,
	overview,
	popularity,
	poster_path,
	production_companies,
	production_countries,
	release_date,
	revenue,
	runtime,
	spoken_languages,
	status,
	tagline,
	title,
	video,
	vote_average,
	vote_count
)
FROM '/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/movies_metadata.csv'
DELIMITER ','
CSV HEADER;
/* --------- ratings */
CREATE TABLE IF NOT EXISTS public.ratings
(
	userId INTEGER,
	movieId INTEGER,
	rating FLOAT4,
	timestamp INTEGER
)
WITH (
    OIDS = FALSE
);

ALTER TABLE public.ratings
    OWNER to postgres;


COPY public.ratings(
	userId,
	movieId,
	rating,
	timestamp
)
FROM '/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/ratings.csv'
DELIMITER ','
CSV HEADER;

/* --------- ratings_small */
CREATE TABLE IF NOT EXISTS public.ratings_small
(
	userId INTEGER,
	movieId INTEGER,
	rating FLOAT4,
	timestamp INTEGER
)
WITH (
    OIDS = FALSE
);

ALTER TABLE public.ratings_small
    OWNER to postgres;


COPY public.ratings_small(
	userId,
	movieId,
	rating,
	timestamp
)
FROM '/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/ratings_small.csv'
DELIMITER ','
CSV HEADER;