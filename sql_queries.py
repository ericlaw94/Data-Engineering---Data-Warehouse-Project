import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionID INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userID INTEGER,
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
     num_songs INTEGER,
     artist_id VARCHAR, 
     artist_latitude FLOAT,
     artist_longitude FLOAT,
     artist_location VARCHAR,
     artist_name VARCHAR,
     song_id VARCHAR,
     title VARCHAR,
     duration FLOAT,
     year INTEGER,
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL REFERENCES users (user_id),
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL REFERENCES song (song_id),
    artist_id VARCHAR NOT NULL REFERENCES artists (artist_id),
    session_id INTEGER NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
);

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY SORTKEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    level VARCHAR NOT NULL
);

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(
    song_id VARCHAR PRIMARY KEY SORTKEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    duration FLOAT NOT NULL,
);


""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY SORTKEY,
    name VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY
    hour VARCHAR NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
);

""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events_table from 's3://udacity-dend/log_data'
                           iam_role {}
                           json 's3://udacity-dend/log_json_path.json' ;
                       """).format(config['IAM_ROLE']['ARN'])

staging_songs_copy = (""" copy staging_songs_table from 's3://udacity-dend/song_data'
                          iam_role {}
                          json 'auto';
                     """).format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time ,user_id,level,
                        song_id,artist_id,session_id ,user_agent ,location )
                        SELECT DISTINCT timestamp 'epoch' + se.ts * interval '0.001 seconds' as start_time,
                        se.userId,se.level,
                        ss.song_id,ss.artist_id,
                        se.sessionId,se.userAgent,
                        se.location
                        FROM staging_events se
                        LEFT JOIN staging_songs ss
                        ON se.artist=ss.artist_name
                        AND
                        se.song=ss.title
                        AND
                        se.length=ss.duration
                        WHERE se.page='NextSong' AND se.userId IS NOT NULL
""")


user_table_insert = ("""INSERT INTO users(user_id, first_name,last_name ,gender ,level )
                     SELECT DISTINCT userId, firstName,lastName,gender,level
                     FROM staging_events 
                     WHERE  userId IS NOT NULL
                     AND
                     page = 'NextSong';
""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year,duration)
                    SELECT DISTINCT song_id, title, artist_id, year, duration
                    FROM staging_songs
                    WHERE song_id IS NOT NULL;


""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
       SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       EXTRACT(WEEKDAY FROM start_time) AS weekday
       FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
