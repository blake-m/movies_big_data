"""A MapReduce job joining 2 tables to get Movie titles of movies with
average rating of at least 4, with at least 500 ratings, ordered from the most
highly rated to the least highly rated."""

import csv
import io

from mrjob import job
from mrjob import step


class MostPopularRatingAbove4(job.MRJob):
    def steps(self):
        steps_list = [
            step.MRStep(mapper=self.map_divide_inputs_into_2_tables,
                        reducer=self.reduce_group_both_tables_by_movie_id),
            step.MRStep(reducer=self.reduce_join_tables_by_movie_id),
            step.MRStep(reducer=self.reduce_get_avg_ratings_and_rating_counts),
            step.MRStep(reducer=self.reduce_order_by_rating)
        ]
        return steps_list

    def map_divide_inputs_into_2_tables(self, _, line):
        data = io.StringIO(line)
        reader = csv.reader(data, delimiter=',')
        for row in reader:
            if len(row) == 24:
                # Column names are:
                # adult, belongs_to_collection, budget, genres, homepage, id,
                # imdb_id, original_language, original_title, overview,
                # popularity, poster_path, production_companies,
                # production_countries, release_date, revenue, runtime,
                # spoken_languages, status, tagline, title,
                # video, vote_average, vote_count
                columns = row
                id_ = columns[5]
                title = columns[20]
                yield 'movies_id_and_title', (id_, title)
            # len(row[2]) < 4 - this gets rid of broken data
            elif len(row) == 4 and len(row[2]) < 4:
                # Column names are:
                # userId, movieId, rating, timestamp
                columns = row
                movie_id = columns[1]
                rating = columns[2]
                yield 'ratings', (movie_id, (1, rating))
            else:
                yield 'others', 1

    def reduce_group_both_tables_by_movie_id(self, key, columns):
        cols = list(columns)
        if key == 'movies_id_and_title':
            for col in cols:
                movie_id = col[0]
                title = col[1]
                yield movie_id, title
        if key == 'ratings':
            for col in cols:
                movie_id = col[0]
                count_and_rating = col[1]
                yield movie_id, count_and_rating

    def reduce_join_tables_by_movie_id(self, key, columns):
        cols = list(columns)
        title = cols[0]
        ratings = cols[1:]
        if len(ratings) > 0 and type(title) != list:
            yield title, ratings

    def reduce_get_avg_ratings_and_rating_counts(
            self, movie_title, count_rating_pair):
        """"""
        counts = 0
        avg_rating = 0
        try:
            counts_list = []
            ratings_list = []
            list_of_count_rating_pairs = list(count_rating_pair)
            for pairs in list_of_count_rating_pairs:
                for pair in pairs:
                    if type(pair[1]) == list:
                        yield None, pair
                        break
                    counts_list.append(pair[0])
                    ratings_list.append(float(pair[1]))
            counts = sum(counts_list)
            avg_rating = sum(ratings_list) / len(ratings_list)
        except ValueError:
            # The csv files have headers.
            pass
        if counts >= 500 and avg_rating >= 4.0:
            # Send all movies with at least 100 ratings to 1 reducer
            # avg_rating is the first arg to sort by it easily in the next step
            yield None, (avg_rating, movie_title, counts)

    def reduce_order_by_rating(self, _, titles_counts_avg_triples):
        list_of_titles_counts_and_averages = list(titles_counts_avg_triples)
        results = sorted(
            list_of_titles_counts_and_averages,
            reverse=True  # Descending
        )
        for result in results:
            movie_title = result[1]
            average_rating = result[0]
            ratings_count = result[2]
            yield movie_title, (average_rating, ratings_count)


if __name__ == '__main__':
    MostPopularRatingAbove4.run()
