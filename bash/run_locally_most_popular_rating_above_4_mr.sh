#!/usr/bin/env bash

PYTHONSCRIPT="/home/blake/PycharmProjects/BigDataMovies/python/most_popular_rating_above_4_mr.py"
RATINGSCSV="/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/ratings.csv"
MOVIESCSV="/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/movies_metadata.csv"

python  $PYTHONSCRIPT -r local $RATINGSCSV $MOVIESCSV
