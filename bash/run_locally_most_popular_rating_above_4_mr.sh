#!/usr/bin/env bash

PYTHONSCRIPT="/home/blake/PycharmProjects/BigDataMovies/python/most_popular_rating_above_4_mr.py"
DATACSV="/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/ratings.csv"

python  $PYTHONSCRIPT -r local $DATACSV