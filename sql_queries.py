import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """
DROP TABLE IF EXISTS "staging_events";
"""
staging_songs_table_drop = """
DROP TABLE IF EXISTS "staging_songs";
"""
songplay_table_drop = """
DROP TABLE IF EXISTS "songplays";
"""
user_table_drop = """
DROP TABLE IF EXISTS "users";
"""
song_table_drop = """
DROP TABLE IF EXISTS "songs";
"""
artist_table_drop = """
DROP TABLE IF EXISTS "artists";
"""
time_table_drop = """
DROP TABLE IF EXISTS "times";
"""

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events" (
    "artist"  character varying (255),
    "auth" character varying (11),
    "firstName" character varying (255),
    "gender" char (1),
    "itemiInSession" integer NOT NULL,
    "lastName" character varying (255),
    "length" double precision,
    "level" character varying (4) NOT NULL,
    "location" character varying (255),
    "method" character varying (4) NOT NULL,
    "page" character varying (255) NOT NULL,
    "registration" double precision,
    "sessionId" integer NOT NULL,
    "song" character varying (255),
    "status" integer NOT NULL,
    "ts" numeric NOT NULL,
    "userAgent" character varying (255),
    "userId" integer 
);
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
    "num_songs" integer NOT NULL,
    "artist_id"  character varying (255) NOT NULL,
    "artist_latitude" double precision ,
    "artist_longitude" double precision,
    "artist_location" character varying (255),
    "artist_name" character varying (255) NOT NULL,
    "song_id" character varying (255) NOT NULL,
    "title" character varying (255) NOT NULL,
    "duration" double precision,
    "year" integer NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE "songplays" (
    "songplay_id" int identity(0, 1),
    "start_time" timestamp NOT NULL,
    "user_id" integer,
    "level"  character varying (4) NOT NULL,
    "song_id" character varying (255),
    "artist_id" character varying (255),
    "sessionId" integer NOT NULL,
    "location" character varying (255),
    "userAgent" character varying (255)
);
""")

user_table_create = ("""
CREATE TABLE "users" (
    "user_id" integer NOT NULL,
    "firstName" character varying (255),
    "lastName" character varying (255),
    "gender" character varying (1),
    "level" character varying (4) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE "songs" (
    "song_id" character varying (255) NOT NULL,
    "title" character varying (255) NOT NULL,
    "artist_id" character varying (255) NOT NULL,
    "year" integer NOT NULL,
    "duration" double precision
);
""")

artist_table_create = ("""
CREATE TABLE "artists" (
    "artist_id"  character varying (255) NOT NULL,
    "name" character varying (255),
    "location" character varying (255),
    "artist_latitude" double precision ,
    "artist_longitude" double precision
);
""")

time_table_create = ("""
CREATE TABLE "times" (
    "start_time" timestamp NOT NULL,
    "hour" integer NOT NULL,
    "day" integer NOT NULL,
    "week" integer NOT NULL,
    "month" integer NOT NULL,
    "year" integer NOT NULL,
    "weekday" integer NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
iam_role '{}'
format as json {}
""").format( config.get("S3", "LOG_DATA"),
             config.get("IAM_ROLE", "ARN"),
             config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
copy staging_songs from {}
iam_role '{}'
json 'auto'
""").format(config.get("S3", "SONG_DATA"), 
            config.get("IAM_ROLE", "ARN"))


# FINAL TABLES

songplay_table_insert = ("""
insert into songplays 
    (
        start_time, user_id, level, song_id, artist_id,
        sessionId, location, userAgent
    )
    select
        timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    from staging_events e
    left join staging_songs s on e.song = s.title and e.artist = s.artist_name
    where e.page = 'NextSong'
""")

user_table_insert = ("""
insert into users
    select eo.userId, eo.firstName, eo.lastName, eo.gender, eo.level
    from staging_events eo
    join (
        select max(ts) as ts, userId
        from staging_events
        where page = 'NextSong'
        group by userId
    ) ei on eo.userId = ei.userId and eo.ts = ei.ts
""")

song_table_insert = ("""
insert into songs
    select
        song_id,
        title,
        artist_id,
        year,
        duration
    from staging_songs
""")

artist_table_insert = ("""
insert into artists
    select distinct
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from staging_songs
""")

time_table_insert = ("""
insert into times
    select
        ti.start_time,
        extract(hour from ti.start_time) as hour,
        extract(day from ti.start_time) as day,
        extract(week from ti.start_time) as week,
        extract(month from ti.start_time) as month,
        extract(year from ti.start_time) as year,
        extract(weekday from ti.start_time) as weekday
    from (
        select distinct
            timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
        from staging_events
        where page = 'NextSong'
    ) ti
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
