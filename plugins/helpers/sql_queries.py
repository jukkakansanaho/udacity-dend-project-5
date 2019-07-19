class SqlQueries:
    """ SQL queries for ETL pipeline orchestrated with Apache Airflow.
    Queries:
    * songplay_table_insert_delete  -- delete and insert data to songplay table
    * user_table_insert_delete      -- delete and insert data to user table
    * song_table_insert_delete      -- delete and insert data to song table
    * artist_table_insert_delete    -- delete and insert data to artist table
    * time_table_insert_delete      -- delete and insert data to time table

    * songplay_table_insert_append  -- append data to songplay table
    * user_table_insert_append      -- append data to user table
    * song_table_insert_append      -- append data to song table
    * artist_table_insert_append    -- append data to artist table
    * time_table_insert_append      -- append data to time table

    * songplays_check_nulls         -- check that no nulls in songplays table
    * user_check_nulls              -- check that no nulls in user table
    * song_check_nulls              -- check that no nulls in song table
    * artist_check_nulls            -- check that no nulls in artist table
    * time_check_nulls              -- check that no nulls in time table

    * songplays_check_count         -- count rows in songplays table
    * user_check_count              -- count rows in user table
    * song_check_count              -- count rows in song table
    * artist_check_count            -- count rows in artist table
    * time_check_count              -- count rows in time table
    """

    # TRUNCATE-INSERT SQL queries:
    songplay_table_insert_delete = ("""
        SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
        FROM staging_events AS se
        JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name
            AND se.artist = ss.artist_name
            AND se.length = ss.duration)
        WHERE se.page = 'NextSong';
    """)

    user_table_insert_delete = ("""
        SELECT  DISTINCT se.userId      AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
        FROM staging_events AS se
        WHERE se.page = 'NextSong';
    """)

    song_table_insert_delete = ("""
        SELECT  DISTINCT ss.song_id     AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
        FROM staging_songs AS ss;
    """)

    artist_table_insert_delete = ("""
        SELECT  DISTINCT ss.artist_id   AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
        FROM staging_songs AS ss;
    """)

    time_table_insert_delete = ("""
        SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
        FROM    staging_events               AS se
        WHERE se.page = 'NextSong';
    """)

    # APPEND SQL queries:
    songplay_table_insert_append = ("""
        SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
        FROM staging_events             AS se
        JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name
            AND se.artist = ss.artist_name
            AND se.length = ss.duration)
        WHERE se.page = 'NextSong'
            AND NOT EXISTS( SELECT start_time
                            FROM {}
                            WHERE   start_time = {}.user_id)
    """)

    user_table_insert_append = ("""
        SELECT  DISTINCT se.userId      AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
        FROM staging_events             AS se
        WHERE se.page = 'NextSong'
            AND NOT EXISTS( SELECT user_id
                            FROM {}
                            WHERE se.userid = {}.user_id)
    """)

    song_table_insert_append = ("""
        SELECT  DISTINCT ss.song_id     AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
        FROM staging_songs              AS ss
        WHERE NOT EXISTS(   SELECT song_id
                            FROM {}
                            WHERE ss.song_id = {}.song_id)
    """)

    artist_table_insert_append = ("""
        SELECT  DISTINCT ss.artist_id   AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
        FROM staging_songs AS ss
        WHERE NOT EXISTS(   SELECT artist_id
                            FROM {}
                            WHERE ss.artist_id = {}.artist_id)
    """)

    time_table_insert_append = ("""
        SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
        FROM    staging_events               AS se
        WHERE se.page = 'NextSong'
            AND NOT EXISTS( SELECT start_time
                            FROM {}
                            WHERE start_time = {}.start_time)
    """)
    # -------------------------------------------------------
    # Data quality check queries:
    songplays_check_nulls = ("""
        SELECT COUNT(*)
        FROM songplays
        WHERE   songplay_id IS NULL OR
                start_time IS NULL OR
                user_id IS NULL;
    """)

    users_check_nulls = ("""
        SELECT COUNT(*)
        FROM users
        WHERE user_id IS NULL;
    """)

    songs_check_nulls = ("""
        SELECT COUNT(*)
        FROM songs
        WHERE song_id IS NULL;
    """)

    artists_check_nulls = ("""
        SELECT COUNT(*)
        FROM artists
        WHERE artist_id IS NULL;
    """)

    time_check_nulls = ("""
        SELECT COUNT(*)
        FROM time
        WHERE start_time IS NULL;
    """)

    # Data quality check queries:
    songplays_check_count = ("""
        SELECT COUNT(*)
        FROM songplays;
    """)

    users_check_count = ("""
        SELECT COUNT(*)
        FROM users;
    """)

    songs_check_count = ("""
        SELECT COUNT(*)
        FROM songs;
    """)

    artists_check_count = ("""
        SELECT COUNT(*)
        FROM artists;
    """)

    time_check_count = ("""
        SELECT COUNT(*)
        FROM time;
    """)
    # -------------------------------------------------------
    # ORIGINAL SQL Queries:
    songplay_table_insert_orig = ("""
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time,
            events.userid,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionid,
            events.location,
            events.useragent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert_orig = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert_orig = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert_orig = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert_orig = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
