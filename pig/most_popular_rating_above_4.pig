movieMetadata = LOAD '/user/maria_dev/movie_db/movies_metadata.csv'
	USING org.apache.pig.piggybank.storage.CSVLoader() AS (
    	adult: boolean,
    	belongs_to_collection: chararray,
    	budget: int,
    	genres: chararray,
    	homepage: chararray,
    	id: int,
    	imdb_id: chararray,
        original_language: chararray,
        original_title: chararray,
        overview: chararray,
        popularity: float,
        poster_path: chararray,
    	production_companies: chararray,
        production_countries: chararray,
        release_date: chararray,
        revenue: int,
        runtime: int,
        spoken_languages: chararray,
        status: chararray,
        tagline: chararray,
        title: chararray,
        video: boolean,
        vote_average: float,
        vote_count: int
    );


ratings = LOAD '/user/maria_dev/movie_db/ratings.csv'
	USING org.apache.pig.piggybank.storage.CSVLoader() AS (
	    userID: int,
	    movieId: int,
	    rating: float,
	    timestamp: int
	);

movieIDAndTitle = FOREACH movieMetadata
    GENERATE
        id,
        title;

ratingsAndCountsByMovie = GROUP ratings BY movieId;

-- ratings.rating - ratings is a bag containing all grouped data
ratingsAvgAndCounts = FOREACH ratingsAndCountsByMovie
    GENERATE
        group AS id,
        AVG(ratings.rating) AS ratingAvg,
        COUNT(ratings.rating) AS ratingCount;

aboveFourMovies = FILTER ratingsAvgAndCounts
    BY ratingAvg > 4.0
        AND ratingCount > 100;

aboveFourMoviesWTitiles = JOIN
    aboveFourMovies BY id LEFT OUTER,
    movieIDAndTitle BY id;

aboveFourMoviesWTitilesOrdered = ORDER aboveFourMoviesWTitiles
    BY aboveFourMovies::ratingAvg DESC;

DUMP aboveFourMoviesWTitilesOrdered;