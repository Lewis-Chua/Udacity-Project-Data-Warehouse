import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
  artist         VARCHAR(MAX),
  auth           VARCHAR(MAX),
  firstName      VARCHAR(MAX),
  gender         VARCHAR(MAX),
  itemInSession  INTEGER,
  lastName       VARCHAR(MAX),
  length         FLOAT,
  level          VARCHAR(MAX),
  location       VARCHAR(MAX),
  method         VARCHAR(MAX),
  page           VARCHAR(MAX),
  registration   FLOAT,
  sessionId      VARCHAR,
  song           VARCHAR(MAX),
  status         INTEGER,
  ts             BIGINT,
  userAgent      VARCHAR(MAX),
  userId         BIGINT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
  song_id          VARCHAR(MAX),
  num_songs        INTEGER,
  title            VARCHAR(MAX),
  artist_name      VARCHAR(MAX),
  artist_latitude  FLOAT,
  year             INTEGER,
  duration         FLOAT,
  artist_id        VARCHAR(MAX),
  artist_longitude FLOAT,
  artist_location  VARCHAR(MAX)
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay 
(
  songplay_id     INTEGER IDENTITY(1,1) PRIMARY KEY,
  start_time      DATE NOT NULL,
  user_id         VARCHAR,
  level           VARCHAR,
  song_id         VARCHAR,
  artist_id       VARCHAR,
  session_id      VARCHAR,
  location        VARCHAR,
  user_agent      VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
  user_id       VARCHAR PRIMARY KEY,
  first_name    VARCHAR,
  last_name     VARCHAR,
  gender        VARCHAR,
  level         VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table 
(
  song_id     VARCHAR PRIMARY KEY, 
  title       VARCHAR,
  artist_id   VARCHAR,
  year        INTEGER,
  duration    INTEGER
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist 
(
  artist_id     VARCHAR PRIMARY KEY,
  name          VARCHAR,
  location      VARCHAR,
  latitude      FLOAT,
  longitude     FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
  start_time     timestamp PRIMARY KEY,
  hour           INTEGER,
  day            INTEGER,
  week           INTEGER,
  month          INTEGER,
  year           INTEGER,
  weekday        BOOLEAN
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
json {} compupdate on region 'us-west-2';
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
json 'auto' region 'us-west-2';
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select
timestamp 'epoch' +  (se.ts / 1000) * INTERVAL '1 second' as start_time,
se.userId as user_id,
se.level,
ss.song_id,
ss.artist_id,
se.sessionId as session_id,
se.location,
se.userAgent as user_agent
from staging_events se
left join staging_songs ss
    on se.song = ss.title
    and se.artist = ss.artist_name
where se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
select distinct userId as user_id,
firstName as first_name,
lastName as last_name,
gender,
level
from staging_events
where userId is not null
""")

song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
select distinct song_id,
title,
artist_id,
year,
duration
from staging_songs
where song_id is not null
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude)
select distinct artist_id,
artist_name as name,
artist_location as location,
artist_latitude as latitude,
artist_longitude as longitude
from staging_songs
where artist_id is not null
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
with base as (select (timestamp 'epoch' +  (ts / 1000) * INTERVAL '1 second')::timestamp as start_time from staging_events)
select distinct start_time,
EXTRACT(hour FROM start_time) as hour,
EXTRACT(day FROM start_time) as day,
EXTRACT(week FROM start_time) as week,
EXTRACT(month FROM start_time) as month,
EXTRACT(year FROM start_time) as year,
EXTRACT(weekday FROM start_time) as weekday
-- CASE WHEN EXTRACT(ISODOW FROM start_time) NOT IN (6, 7) THEN true ELSE false END AS weekday
from base
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
