import sqlalchemy
from pprint import pprint


def homework_requests(engine_connection):
    print('-------')
    pprint(engine_connection.execute(
        """SELECT name, release_year FROM albums
        WHERE release_year = 2018
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT name, duration FROM tracks
        ORDER BY duration DESC
        """
    ).fetchone())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT name FROM tracks
        WHERE duration >= 3.30
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT name FROM collections
        WHERE release_year BETWEEN 2018 AND 2020
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT name FROM singers
        WHERE name NOT LIKE '%% %%'
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT name FROM tracks
        WHERE name iLIKE '%%my%%'
        """
    ).fetchall())


if __name__ == '__main__':
    database = 'postgresql://postgres:K9091889r@localhost:5432/postgres'
    engine = sqlalchemy.create_engine(database)
    connection = engine.connect()

    homework_requests(connection)