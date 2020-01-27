from mrjob import job
from mrjob import step


class MostPopularRatingAbove4(job.MRJob):
    def steps(self):
        steps_list = [
            step.MRStep(mapper=self.mapper_get_user_id_and_ratings,
                        reducer=self.reducer_average_the_ratings)
        ]
        return steps_list

    def mapper_get_user_id_and_ratings(self, _, line: str):
        user_id, movie_id, rating, timestamp = line.split(',')
        yield user_id, rating

    def reducer_average_the_ratings(self, user_id, ratings):
        avg_rating = 0
        try:
            ratings_list = list(ratings)
            ratings_list = list(map(float, ratings_list))
            avg_rating = sum(ratings_list) / len(ratings_list)
        except ValueError as error:
            print('Skipping a line because of the following error:')
            print('\t', error)
        yield user_id, avg_rating


if __name__ == '__main__':
    MostPopularRatingAbove4.run()
