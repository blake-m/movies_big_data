import starbase

_starbase_connection = starbase.client.connection.Connection
_starbase_table = starbase.client.table.Table
# ratings = conn.table("ratings_small")
#

#
# ratings.create("")
#
# ratings_small_path = "the-movies-dataset/ratings_small.csv"
# with open(ratings_small_path, "r") as rs:
#     pass


def establish_and_return_hbase_connection(
        ip: str = "127.0.0.1", port: str = "8000") -> _starbase_connection:
    """Establishes HBase RESTful connection using starbase library."""
    conn = starbase.Connection(ip, port)
    return conn


def create_hbase_table(
        conn: _starbase_connection, table_name: str) -> _starbase_table:
    """Creates an HBase table with specified name. Drops such table if it
    exists already."""
    table = conn.table(table_name)
    if table.exists():
        print("Dropping existing {} table\n".format(table_name))
        table.drop()
    return table


def create_column_family(table: _starbase_table, family_name: str) -> None:
    """"""
    table.create(family_name)


def main():
    conn = establish_and_return_hbase_connection()
    ratings = create_hbase_table(conn, "ratings_small")
    create_column_family(ratings, family_name="rating")


if __name__ == "__main__":
    main()
