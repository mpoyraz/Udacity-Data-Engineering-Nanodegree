import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
iam_role_arn = config.get('IAM_ROLE', 'arn')
s3_log_data_path = config.get('S3', 'log_data')
s3_log_json_path = config.get('S3', 'log_jsonpath')
s3_song_data_path = config.get('S3', 'song_data')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("CREATE TABLE IF NOT EXISTS stage_events \
                              (artist varchar, \
                               auth varchar, \
                               firstName varchar, \
                               gender varchar, \
                               itemInSession int, \
                               lastName varchar, \
                               length decimal, \
                               level varchar, \
                               location varchar, \
                               method varchar, \
                               page varchar, \
                               registration decimal(25, 5), \
                               sessionId int, \
                               song varchar, \
                               status int, \
                               ts bigint, \
                               userAgent varchar, \
                               userId int);")

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS stage_songs \
                              (num_songs int, \
                               artist_id varchar, \
                               artist_latitude decimal, \
                               artist_longitude decimal, \
                               artist_location varchar, \
                               artist_name varchar, \
                               song_id varchar, \
                               title varchar, \
                               duration decimal, \
                               year smallint);")

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays \
                         (songplay_id bigint identity(0, 1) PRIMARY KEY, \
                          start_time timestamp NOT NULL REFERENCES time(start_time), \
                          user_id int NOT NULL REFERENCES users(user_id), \
                          level varchar NOT NULL, \
                          song_id varchar REFERENCES songs(song_id), \
                          artist_id varchar REFERENCES artists(artist_id), \
                          session_id int NOT NULL, \
                          location varchar, \
                          user_agent varchar );")

user_table_create = ("CREATE TABLE IF NOT EXISTS users \
                     (user_id int PRIMARY KEY, \
                      first_name varchar NOT NULL, \
                      last_name varchar NOT NULL, \
                      gender varchar, \
                      level varchar NOT NULL);")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs \
                      (song_id varchar PRIMARY KEY, \
                       title varchar NOT NULL, \
                       artist_id varchar NOT NULL, \
                       year smallint, \
                       duration decimal );")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists \
                        (artist_id varchar PRIMARY KEY, \
                         name varchar NOT NULL, \
                         location varchar, \
                         latitude decimal, \
                         longitude decimal );")

time_table_create = ("CREATE TABLE IF NOT EXISTS time \
                     (start_time timestamp PRIMARY KEY, \
                      hour smallint NOT NULL, \
                      day smallint NOT NULL, \
                      week smallint NOT NULL, \
                      month smallint NOT NULL,\
                      year smallint NOT NULL, \
                      weekday smallint NOT NULL);")

# STAGING TABLES
staging_events_copy = ("""copy stage_events from {}
                          iam_role {}
                          json {};
                       """).format(s3_log_data_path, iam_role_arn, s3_log_json_path)

staging_songs_copy = ("""copy stage_songs from {}
                          iam_role {}
                          json 'auto';
                      """).format(s3_song_data_path, iam_role_arn)

# FINAL TABLES
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT userId, firstName, lastName, gender, level FROM stage_events
                        WHERE concat(userId, ts) IN (SELECT concat(userId, MAX(ts)) FROM stage_events
                                                     WHERE userId IS NOT NULL
                                                     GROUP BY userId)
                     """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT stage_ts.start_time,
                               EXTRACT(hour from stage_ts.start_time),
                               EXTRACT(day from stage_ts.start_time),
                               EXTRACT(week from stage_ts.start_time),
                               EXTRACT(month from stage_ts.start_time),
                               EXTRACT(year from stage_ts.start_time),
                               EXTRACT(dayofweek from stage_ts.start_time)
                        FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time FROM stage_events) AS stage_ts
                     """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration FROM stage_songs
                     """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM stage_songs
                       """)

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second',
                                   e.userId,
                                   e.level,
                                   s.song_id,
                                   a.artist_id,
                                   e.sessionId,
                                   e.location,
                                   e.userAgent
                            FROM stage_events e
                            JOIN songs s ON e.song = s.title
                            JOIN artists a ON e.artist = a.name
                            WHERE e.page = 'NextSong' AND e.userId IS NOT NULL
                         """)

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, time_table_create, song_table_create, artist_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, time_table_drop, song_table_drop, artist_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, time_table_insert, song_table_insert, artist_table_insert, songplay_table_insert]
