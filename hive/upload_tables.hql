CREATE TABLE IF NOT EXISTS movieRatings(
	    userID INT,
	    movieId INT,
	    rating FLOAT,
	    timestamp_ INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/maria_dev/movie_db/ratings.csv'
OVERWRITE INTO TABLE movieRatings;

CREATE TABLE IF NOT EXISTS moviesMetadata(
    	adult BOOLEAN,
    	belongs_to_collection STRING,
    	budget INT,
    	genres STRING,
    	homepage STRING,
    	id INT,
    	imdb_id STRING,
        original_language STRING,
        original_title STRING,
        overview STRING,
        popularity FLOAT,
        poster_path STRING,
    	production_companies STRING,
        production_countries STRING,
        release_date STRING,
        revenue INT,
        runtime INT,
        spoken_languages STRING,
        status STRING,
        tagline STRING,
        title STRING,
        video BOOLEAN,
        vote_average FLOAT,
        vote_count INT
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"")
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/maria_dev/movie_db/movies_metadata.csv'
OVERWRITE INTO TABLE movieRatings;