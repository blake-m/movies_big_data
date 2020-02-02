/* Imports ratings_small.csv data set into HBase table using the following structure:

    Unique row identifier - "user_id"
    column family - "ratings"
        columns are: "movie_id" and data in them the "rating" itself

NOTE: HBase table needs to be created ahead of time, before this script is running.
    To create a table simply log into HBase shell using "hbase shell" command
    and type in create "<table_name>","<column_family>".

    In our case the code used to create the table and column family was:
        create 'user_ratings','movie_ratings'

 */


ratingsByUser = LOAD '/user/maria_dev/movie_db/ratings_small.csv'
    USING org.apache.pig.piggybank.storage.CSVLoader() AS (
        userId: int,
        movieId: int,
        rating: float
    );

STORE ratingsByUser INTO 'hbase://user_ratings'
    USING org.apache.pig.backend.hadoop.hbase.HBaseStorage (
        'movie_ratings:movieId,movie_ratings:rating'
    );