import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

ARN=config.get('IAM_ROLE', 'ARN')
LOG_DATA=config.get('S3', 'LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3', 'SONG_DATA')

# drop tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# create tables
# sorting keys?
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    ItemInSession VARCHAR,
    lastName VARCHAR,
    length VARCHAR,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId VARCHAR,
    song VARCHAR,
    status VARCHAR,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    artist_id VARCHAR NOT NULL,
    artist_latitude NUMERIC,
    artist_location VARCHAR,
    artist_longitude NUMERIC,
    artist_name VARCHAR,
    duration NUMERIC,
    num_songs INT,
    song_id VARCHAR NOT NULL,
    title VARCHAR,
    year INT
);
""")

# possibly diststyle all
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL,
    UNIQUE (start_time,user_id, session_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    level VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY NOT NULL,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT NOT NULL,
    duration NUMERIC NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY NOT NULL,
    artist_name VARCHAR NOT NULL,
    location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY NOT NULL,
    hour INT NOT NULL,
    day INT NOT NULL,
    week VARCHAR NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);
""")

# staging tables
staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
json {}
TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto';
""").format(SONG_DATA, ARN)

# WHERE action is nextsong
# join for song id??
# end tables inserts
songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id,
artist_id, session_id, location, user_agent)
SELECT e.ts AS start_time,
CAST(e.userId AS INTEGER) AS user_id,
e.level,
s.song_id,
s.artist_id,
CAST(e.sessionId AS INTEGER) AS session_id,
e.location,
e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s
ON s.title = e.song
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name,
last_name, gender, level)
SELECT CAST(userId AS INTEGER) AS user_id,
firstName AS first_name,
lastName AS last_name,
gender, level
FROM staging_events
WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
title,
artist_id,
year,
duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, artist_name, location, latitude, longitude)
SELECT DISTINCT artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
SELECT ts,
EXTRACT(hour FROM ts) AS hour,
EXTRACT(day FROM ts) AS day,
EXTRACT(week FROM ts) AS week,
EXTRACT(month FROM ts) AS month,
EXTRACT(year FROM ts) AS year,
EXTRACT(weekday FROM ts) AS weekday
FROM staging_events
WHERE page = 'NextSong';
""")

# analytical queries
pop_artists = ("""
SELECT a.artist_name
FROM artists a
JOIN songplays s 
ON a.artist_id = s.artist_id
LIMIT 10;
""")

songs = ("""
SELECT COUNT(DISTINCT(s.title))
FROM songs s
JOIN songplays sp
ON s.song_id = sp.song_id
""")


# queries
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analytics_table_queries = [pop_artists, songs]
