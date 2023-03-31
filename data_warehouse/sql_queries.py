import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE staging_events_table
(       artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          VARCHAR,
        itemInSession   INTEGER,
        lastName        VARCHAR,
        length          DECIMAL(10,5),
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    BIGINT,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              VARCHAR,
        userAgent       VARCHAR,
        userId          INTEGER
);
""")

staging_songs_table_create = (""" CREATE TABLE staging_songs_table
(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     DECIMAL(18,15),
        artist_longitude    DECIMAL(18,15),
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            DECIMAL(10,5),
        year                INTEGER
)
""")

songplay_table_create = (""" CREATE TABLE songplays
(
        songplay_id       INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time        timestamp SORTKEY NOT NULL REFERENCES time(start_time),
        user_id           INTEGER NOT NULL REFERENCES users(user_id),
        level             VARCHAR,
        song_id           VARCHAR NOT NULL REFERENCES songs(song_id),
        artist_id         VARCHAR NOT NULL REFERENCES artists(artist_id),
        session_id        INTEGER,
        location          VARCHAR,
        user_agent        VARCHAR              
)
""")

user_table_create = (""" CREATE TABLE users
(
        user_id           INTEGER PRIMARY KEY DISTKEY,
        first_name        VARCHAR,
        last_name         VARCHAR,
        gender            VARCHAR,
        level             VARCHAR                         
)
""")

song_table_create = (""" CREATE TABLE songs
(
        song_id           VARCHAR PRIMARY KEY,
        title             VARCHAR,
        artist_id         VARCHAR,
        year              INTEGER,
        duration          DECIMAL(10,5)           
)
""")

artist_table_create = (""" CREATE TABLE artists
(
        artist_id         VARCHAR PRIMARY KEY,
        name              VARCHAR,
        location          VARCHAR,
        latitude          DECIMAL(18,15),
        longitude         DECIMAL(18,15)
)
""")

time_table_create = (""" CREATE TABLE time
(
        start_time        timestamp PRIMARY KEY,
        hour              INTEGER,
        day               INTEGER,    
        week              INTEGER,
        month             INTEGER,
        year              INTEGER,
        weekday           INTEGER
)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events_table
from {}
iam_role {}
json {}
region 'us-west-2';""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH']))

staging_songs_copy = (""" COPY staging_songs_table 
from {}
iam_role {}
format as json 'auto'
region 'us-west-2';""".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN']))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time, 
                        user_id, level, song_id, artist_id, session_id, 
                        location, user_agent)
                        SELECT DISTINCT timestamp 'epoch' \
                        + CAST(ts AS BIGINT)/1000 * interval '1 second' as start_time,
                        e.userId as user_id,
                        e.level as level,
                        s.song_id as song_id,
                        s.artist_id as artist_id,
                        e.sessionId as session_id,
                        e.location as location,
                        e.userAgent as user_agent
                        FROM staging_songs_table s 
                        JOIN staging_events_table e ON \
                        (s.title = e.song AND e.length = s.duration) 
                        WHERE e.page = 'NextSong' AND 
                        e.ts IS NOT NULL AND
                        e.userId IS NOT NULL AND
                        s.song_id IS NOT NULL AND
                        s.artist_id IS NOT NULL""")


user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events_table 
WHERE userId IS NOT NULL AND
firstName IS NOT NULL AND
lastName IS NOT NULL AND
gender IS NOT NULL AND
level IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) 
SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs_table
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
FROM staging_songs_table 
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                SELECT start_time,
                EXTRACT(hour FROM start_time) as hour,
                EXTRACT(day FROM start_time) as day,
                EXTRACT(week FROM start_time) as week,
                EXTRACT(month FROM start_time) as month,
                EXTRACT(year FROM start_time) as year,
                EXTRACT(weekday FROM start_time) as weekday FROM songplays
                """)

# Testing
test_staging_events = "SELECT COUNT(*) FROM staging_events_table;"
test_songs_events = "SELECT COUNT(*) FROM staging_songs_table;"
test_songplays = "SELECT COUNT(*) FROM songplays;"
test_users = "SELECT COUNT(*) FROM users;"
test_songs = "SELECT COUNT(*) FROM songs;"
test_artists = "SELECT COUNT(*) FROM artists;"
test_time = "SELECT COUNT(*) FROM time;"
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, \
                        user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,\
                       user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
test_queries = [[test_staging_events, "SELECT COUNT (*) FROM staging_events"],
                [test_songs_events, "SELECT COUNT (*) FROM staging_songs"],
                [test_songplays, "SELECT COUNT (*) FROM songplays"],
                [test_users, "SELECT COUNT (*) FROM users"],
                [test_songs, "SELECT COUNT (*) FROM songs"],
                [test_artists, "SELECT COUNT(* FROM artists"],
                [test_time, "SELECT COUNT (*) FROM time"]]

