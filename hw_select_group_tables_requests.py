import sqlalchemy
from pprint import pprint


def group_tables_requests(engine_connection):
    print('-------')
    pprint(engine_connection.execute(
        """SELECT name, COUNT(singer_id) FROM singer_genre
        JOIN genres ON singer_genre.genre_id = genres.id
        GROUP BY name
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT release_year, COUNT(t.id) FROM tracks t
        LEFT JOIN albums a ON t.album_id = a.id
        WHERE release_year BETWEEN 2019 AND 2020
        GROUP BY release_year
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT a.name, AVG(duration) FROM tracks t
        LEFT JOIN albums a ON t.album_id = a.id
        GROUP BY a.name
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT s.name, a.release_year FROM singers s
        LEFT JOIN singer_album sa ON s.id = sa.singer_id
        LEFT JOIN albums a ON sa.album_id = a.id
        WHERE a.release_year != 2020
        GROUP BY s.name, a.release_year
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT c.name, s.name FROM collections c
        LEFT JOIN track_collection tc ON c.id = tc.collection_id
        LEFT JOIN tracks t ON tc.track_id = t.id
        LEFT JOIN albums a ON t.album_id = a.id
        LEFT JOIN singer_album sa ON a.id = sa.album_id
        LEFT JOIN singers s ON sa.singer_id = s.id
        WHERE s.name = 'Gorillaz'
        GROUP BY c.name, s.name
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT a.name FROM albums a
        LEFT JOIN singer_album sa ON a.id = sa.album_id
        LEFT JOIN singers s ON sa.singer_id = s.id
        LEFT JOIN singer_genre sg ON s.id = sg.singer_id
        LEFT JOIN genres g ON sg.genre_id = g.id
        GROUP BY a.name
        HAVING COUNT(g.id) > 1
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT t.name FROM tracks t
        LEFT JOIN track_collection tc ON t.id = tc.track_id
        WHERE tc.track_id IS NULL
        GROUP BY t.name
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT s.name, duration FROM singers s
        LEFT JOIN singer_album sa ON s.id = sa.singer_id
        LEFT JOIN albums a ON sa.album_id = a.id
        LEFT JOIN tracks t ON a.id = t.album_id
        WHERE duration = (SELECT MIN(duration) FROM tracks)
        GROUP BY s.name, duration
        """
    ).fetchall())
    print('-------')
    pprint(engine_connection.execute(
        """SELECT a.name, COUNT(t.album_id) FROM albums a
        LEFT JOIN tracks t ON a.id = t.album_id
        GROUP BY a.name, t.album_id
        HAVING COUNT(t.album_id) = (SELECT COUNT(t.album_id) FROM tracks t
                                    LEFT JOIN albums a ON a.id = t.album_id
                                    GROUP BY a.name, t.album_id
                                    ORDER BY COUNT(t.album_id)
                                    LIMIT 1)
        """
    ).fetchall())


if __name__ == '__main__':
    database = 'postgresql://postgres:K9091889r@localhost:5432/postgres'
    engine = sqlalchemy.create_engine(database)
    connection = engine.connect()

    group_tables_requests(connection)
