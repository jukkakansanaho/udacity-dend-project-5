# sql_queries.py

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# Staging tables
staging_events_table_create= ("""
    CREATE TABLE public.staging_events (
    	artist varchar(256),
    	auth varchar(256),
    	firstName varchar(256),
    	gender varchar(256),
    	itemInSession int4,
    	lastName varchar(256),
    	length numeric(18,0),
    	"level" varchar(256),
    	location varchar(256),
    	"method" varchar(256),
    	page varchar(256),
    	registration numeric(18,0),
    	sessionId int4,
    	song varchar(256),
    	status int4,
    	ts int8,
    	userAgent varchar(256),
    	userId int4
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE public.staging_songs (
    	num_songs int4,
    	artist_id varchar(256),
    	artist_name varchar(256),
    	artist_latitude numeric(18,0),
    	artist_longitude numeric(18,0),
    	artist_location varchar(256),
    	song_id varchar(256),
    	title varchar(256),
    	duration numeric(18,0),
    	"year" int4
    );
""")

# Analytics tables
songplay_table_create = ("""
    CREATE TABLE public.songplays (
    	songplay_id INTEGER IDENTITY(0,1) NOT NULL,
    	start_time timestamp NOT NULL,
    	user_id int4 NOT NULL,
    	"level" varchar(256),
    	song_id varchar(256),
    	artist_id varchar(256),
    	session_id int4,
    	location varchar(256),
    	user_agent varchar(256),
    	CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
    );
""")

user_table_create = ("""
    CREATE TABLE public.users (
    	user_id int4 NOT NULL,
    	first_name varchar(256),
    	last_name varchar(256),
    	gender varchar(256),
    	"level" varchar(256),
    	CONSTRAINT users_pkey PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE public.songs (
    	song_id varchar(256) NOT NULL,
    	title varchar(256),
    	artist_id varchar(256),
    	"year" int4,
    	duration numeric(18,0),
    	CONSTRAINT songs_pkey PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE public.artists (
    	artist_id varchar(256) NOT NULL,
    	name varchar(256),
    	location varchar(256),
    	latitude numeric(18,0),
    	longitude numeric(18,0)
    );
""")

time_table_create = ("""
    CREATE TABLE public."time" (
    	start_time timestamp NOT NULL,
    	"hour" int4,
    	"day" int4,
    	week int4,
    	"month" varchar(256),
    	"year" int4,
    	weekday varchar(256),
    	CONSTRAINT time_pkey PRIMARY KEY (start_time)
    ) ;
""")

# QUERY LISTS
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
