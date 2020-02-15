"""This script imports ratings_small.csv data set to HBase.

HBase connection must be open for this script to be successful at ip and port
specified in 'establish_and_return_hbase_connection' function.
"""
from typing import Union

import starbase

_StarbaseConnection = starbase.client.connection.Connection
_StarbaseTable = starbase.client.table.Table


def establish_and_return_hbase_connection(
        ip: str = "127.0.0.1", port: str = "8000") -> _StarbaseConnection:
    """Establishes HBase RESTful connection using starbase library."""
    print("Establishing connection with {}:{}".format(ip, port))
    conn = starbase.Connection(ip, port)
    print("\tEstablished.\n")
    return conn


def create_hbase_table(
        conn: _StarbaseConnection, table_name: str) -> _StarbaseTable:
    """Creates an HBase table with specified name. Drops such table if it
    exists already."""
    table = conn.table(table_name)
    if table.exists():
        print("Dropping existing {} table\n".format(table_name))
        table.drop()
    print("Table {} created.\n".format(table_name))
    return table


def create_column_family(table: _StarbaseTable, family_name: str) -> None:
    """Creates a column family for a specified HBase table."""
    table.create(family_name)
    print("Column Family '{}' created.".format(family_name))


def add_data_to_hbase_table_in_a_batch(
        table: _StarbaseTable, data_set_path: str) -> None:
    """Creates a batch containing data and uploads it to HBase table.

    Reads a file and line by line adds data to the batch. Using a batch makes
    the transfer way faster.
    """
    print("Preparing data batch to add to HBase table...")
    batch = table.batch()
    with open(data_set_path, "r") as data_set:
        next(data_set)   # Skips header
        for row in data_set:
            user_id, movie_id, rating, timestamp = row.split(",")
            batch.update(user_id, {"rating": {movie_id: rating}})
    batch.commit(finalize=True)
    print("\tAdded.\n")


def fetch_hbase_row_and_print_it(table: _StarbaseTable, key: Union[str, int]) -> None:
    print("Results for key '{}':".format(key))
    if type(key) == int:
        print(table.fetch(int(key)))
    else:
        print(table.fetch(key))


def main():
    data_set_path = "/home/blake/PycharmProjects/BigDataMovies/the-movies-dataset/ratings_small.csv"
    conn = establish_and_return_hbase_connection()
    ratings = create_hbase_table(conn, table_name="ratings_small")
    create_column_family(table=ratings, family_name="rating")
    add_data_to_hbase_table_in_a_batch(
        table=ratings, data_set_path=data_set_path)
    fetch_hbase_row_and_print_it(table=ratings, key=5)


if __name__ == "__main__":
    main()
