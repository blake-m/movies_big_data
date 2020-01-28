from mrjob import job
from mrjob import step


class MostPopularRatingAbove4(job.MRJob):
    def steps(self):
        steps_list = [
            step.MRStep(mapper=self.mapper_get_user_id_and_ratings,
                        reducer=self.reducer_average_and_count_the_ratings),
            step.MRStep(reducer=self.reduce_order_by_rating),
        ]
        return steps_list

    def mapper_get_user_id_and_ratings(self, _, line):
        user_id, movie_id, rating, timestamp = line.split(',')
        yield movie_id, (1, rating)

    def reducer_average_and_count_the_ratings(self, movie_id, ratings):
        counts = 0
        avg_rating = 0
        try:
            counts_list = []
            ratings_list = []
            list_of_counts_ratings_pairs = list(ratings)
            for pair in list_of_counts_ratings_pairs:
                counts_list.append(pair[0])
                ratings_list.append(float(pair[1]))
            counts = sum(counts_list)
            avg_rating = sum(ratings_list) / len(ratings_list)
        except ValueError:
            pass
        if counts >= 100 and avg_rating >= 4.0:
            # Send all movies with at least 100 ratings to 1 reducer
            # avg_rating is the first arg to sort by it easily in the next step
            yield None, (avg_rating, movie_id, counts)

    def reduce_order_by_rating(self, _, m_id_counts_avg_triples):
        list_of_m_id_counts_avg_triples = list(m_id_counts_avg_triples)
        results = sorted(list_of_m_id_counts_avg_triples, reverse=True) # Descending
        for result in results:
            yield None, (result[1], result[0], result[2])


if __name__ == '__main__':
    MostPopularRatingAbove4.run()
