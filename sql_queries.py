import configparser

config = configparser.ConfigParser()
config.read(dwh.cfg)

# drop tables
staging_events_table_drop = "DROP TABLE IF EXSITS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# create tables
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS events (
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
ItemInSession INT,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
page VARCHAR,
registration INT,
sessionid INT,
song VARCHAR,
status INT,
ts TIMESTAMP,
userAgent VARCHAR,
userid INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
artist_id VARCHAR NOT NULL,
artist_latitude INT,
artist_longitude INT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR NOT NULL,
title VARCHAR,
duration INT,
year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
songplay_id INT PRIMARY KEY NOT NULL,
start_time INT NOT NULL,
user_id INT FOREIGN KEY NOT NULL,
level INT NOT NULL,
song_id INT NOT NULL,
artist_id INT NOT NULL,
session_id INT NOT NULL,
location FLOAT NOT NULL,
user_agent VARCHAR NOT NULL
UNIQUE(start_time,user_id, session_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id INT PRIMARY KEY NOT NULL,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender VARCHAR NOT NULL,
level INT NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id INT PRIMARY KEY NOT NULL,
title VARCHAR NOT NULL,
artist_id FOREIGN KEY NOT NULL,
year INT NOT NULL,
duration DECIMAL NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id INT PRIMARY KEY NOT NULL,
artist_name VARCHAR NOT NULL,
location VARCHAR,
latitude FLOAT,
longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time DATETIME PRIMARY KEY NOT NULL,
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
COPY events FROM {}
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
""").format(LOG_DATA, ARN)

staging_songs_copy = ("""
COPY songs FROM {}
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
""").format(SONG_DATA, ARN)

# WHERE action is nextsong
# join for song id??
# end tables inserts
songplay_table_insert = ("""
INSERT INTO songplay
(start_time, user_id, level, song_id,
artist_id, session_id, location, user_agent)
SELECT e.ts AS start_time, e.userid AS user_id,
e.level, s.song_id,
s.artist_id, e.sessionid AS session_id,
e.location, e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s
ON s.title = e.song
WHERE page = 'NextSong'
ON CONFLICT ON CONSTRAINT songplay_pkey
DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name,
last_name, gender, level)
SELECT userid AS user_id,
firstName AS first_name,
lastName AS last_name,
gender, level
FROM staging_events
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
ON CONFLICT ON CONSTRAINT songs_pkey
DO_NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, artist_name, location, latitude, longitude)
SELECT artist_id, artist_name,
artist_location, artist_latitude, artist_longitude
FROM staging_songs
ON CONFLICT ON CONTSTAINT artist_pkey
DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, year, weekday)
SELECT ts AS start_time,
EXTRACT(hour FROM ts) AS hour,
EXTRACT(day FROM ts) AS day,
EXTRACT(week FROM ts) AS week,
EXTRACT(year FROM ts) AS year,
EXTRACT(weekday FROM ts) AS weekday
FROM staging_events
WHERE page = 'NextSong'
ON CONFLICT ON CONSTRAINT time_pkey
DO NOTHING;
""")

# queries
create_table_queries = [staging_songs_table_create, staging_songs_table_create, songplay_table_create, user_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_event_copy , staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


# end tables inserts used in local postgres db
# songplay_table_insert = ("""
# INSERT INTO songplay
# (start_time, user_id, level, song_id,
# artist_id, session_id, location, user_agent)
# VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
# ON CONFLICT ON CONSTRAINT songplay_pkey
# DO NOTHING;
# """)
#
# user_table_insert = ("""
# INSERT INTO user
# (user_id, first_name, last_name, gender, level)
# VALUES (%s, %s, %s, %s, %s)
# ON CONFLICT ON CONSTRAINT user_pkey
# DO UPDATE SET level=EXCLUDED.level;
# """)
#
# song_table_insert = ("""
# INSERT INTO song
# (song_id, title, artist_id, year, duration)
# VALUES (%s, %s, %s, %s, %s)
# ON CONFLICT ON CONSTRAINT song_pkey
# DO_NOTHING;
# """)
#
# artist_table_insert = ("""
# INSERT INTO artist
# (artist_id, artist_name, location, latitude, longitude)
# VALUES (%s, %s, %s, %s, %s)
# ON CONFLICT ON CONTSTAINT artist_pkey
# DO NOTHING;
# """)
#
# time_table_insert = ("""
# INSERT INTO time
# (start_time, hour, day, week, year, weekday)
# VALUES (%s, %s, %s, %s, %s, %s, %s)
# ON CONFLICT ON CONSTRAINT time_pkey
# DO NOTHING;
# """)
