import sqlalchemy


band_list = ['The Beatles', 'Queen', 'Eminem', 'Gorillaz', 'Boyzone', 'Everlife', 'Alabama', 'Truck Stop',
             'Luis Russell', 'Artie Shaw and His Orchestra']

genre_list = ['Rock', 'Hip-Hop', 'Pop', 'Country', 'Jazz']

albums_release_dict = {
    'I Kissed A Girl': 2008,
    'Thinking Of You': 2009,
    'Unplugged': 2009,
    'Teenage Dream': 2010,
    'This Is How We Do': 2014,
    'Rise': 2018,
    'Rise Remixes': 2016,
    'Smile': 2020
}

track_duration_dict = {
    'Back in the U.S.S.R.': 2.43,
    'Dear Prudence': 3.05,
    'Glass Onion': 2.43,
    'Ob-La-Di, Ob-La-Da': 2.43,
    'Wild Honey Pie': 2.56,
    'The Continuing Story of Bungalow Bill': 3.52,
    'While My Guitar Gently Weeps': 3.12,
    'Happiness Is a Warm Gun': 2.38,
    'Martha My Dear': 2.55,
    'Blackbird': 3.12,
    'Piggies': 3.47,
    'Rocky Raccoon': 2.59,
    'I Will': 2.36,
    'Julia': 2.43,
    'Birthday': 2.43,
    'Yer Blues': 3.23,
    'Sexy Sadie': 3.39,
    'Helter Skelter': 3.38,
    'Long': 3.42,
    'Revolution': 2.33,
    'Honey Pie': 2.23,
    'Savoy Truffle': 2.53,
    'Cry Baby Cry': 2.46,
    'Revolution 9': 4.03,
    'Good Night': 3.53
}

collections_release_dict = {
    'Chill Out Good Vibes': 2022,
    'Hear Us Roar': 2017,
    'Party Throwback': 2019,
    'Paradise Valley': 2013,
    'Vampire Academy': 2014,
    'Tony Fenton 50 Favourite': 2012,
    'Starstrukk': 2010,
    'When In Rome': 2010
}


def singers_genres_insert(engine_connection, bands_list, genres_list):
    for genre in genres_list:
        engine_connection.execute(
            f"""INSERT INTO genres(name)
            VALUES('{genre}')
            """
        )
    for band in bands_list:
        engine_connection.execute(
            f"""INSERT INTO singers(name)
            VALUES('{band}')
            """
        )
    singers_id = engine_connection.execute(
        f"""SELECT id FROM singers"""
    ).fetchall()
    genres_id = engine_connection.execute(
        f"""SELECT id FROM genres"""
    ).fetchall()
    genre_count = 0
    for id_ in singers_id:
        if genre_count > len(genres_id) - 1:
            genre_count = 0
        engine_connection.execute(
            f"""INSERT INTO singer_genre(singer_id, genre_id)
            VALUES({id_[0]}, {genres_id[genre_count][0]})
            """
        )
        genre_count += 1

def _singers_albums(engine_connection, album_release):
    for album, release_year in album_release.items():
        engine_connection.execute(
            f"""INSERT INTO albums(name, release_year)
            VALUES('{album}', {release_year})
            """
        )
    albums_id = engine_connection.execute(
        """SELECT id FROM albums"""
    ).fetchall()
    singers_id = engine_connection.execute(
        """SELECT id FROM singers"""
    ).fetchall()
    albums_count = 0
    for id_ in singers_id:
        if albums_count > len(albums_id) - 1:
            albums_count = 0
        engine_connection.execute(
            f"""INSERT INTO singer_album(singer_id, album_id)
            VALUES({id_[0]}, {albums_id[albums_count][0]})
            """
        )
        albums_count += 1

    return albums_id

def tracks_collections(engine_connection, track_duration, collection_release, album_release):
    albums_id = _singers_albums(engine_connection, album_release)
    album_id_count = 0
    for track, duration in track_duration.items():
        if album_id_count > len(albums_id) - 1:
            album_id_count = 0
        engine_connection.execute(
            f"""INSERT INTO tracks(name, duration, album_id)
            VALUES('{track}', {duration}, {albums_id[album_id_count][0]})
            """
        )
        album_id_count += 1
    for collection, release_year in collection_release.items():
        engine_connection.execute(
            f"""INSERT INTO collections(name, release_year)
            VALUES('{collection}', {release_year})
            """
        )
    tracks_id = engine_connection.execute(
        """SELECT id FROM tracks"""
    ).fetchall()
    collections_id = engine_connection.execute(
        """SELECT id FROM collections"""
    ).fetchall()
    collections_count = 0
    for id_ in tracks_id:
        if collections_count > len(collections_id) - 1:
            collections_count = 0
        elif id_[0] == 7 or id_[0] == 12 or id_[0] == 13:
            continue
        engine_connection.execute(
            f"""INSERT INTO track_collection(track_id, collection_id)
            VALUES({id_[0]}, {collections_id[collections_count][0]})
            """
        )
        collections_count += 1


if __name__ == '__main__':
    database = 'postgresql://postgres:K9091889r@localhost:5432/postgres'
    engine = sqlalchemy.create_engine(database)
    connection = engine.connect()

    singers_genres_insert(connection, band_list, genre_list)
    tracks_collections(connection, track_duration_dict, collections_release_dict, albums_release_dict)
    print('Заполнение базы данных завершено!')
