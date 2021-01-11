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
DROP TABLE IF EXISTS "songplay";
"""
user_table_drop = """
DROP TABLE IF EXISTS "user";
"""
song_table_drop = """
DROP TABLE IF EXISTS "song";
"""
artist_table_drop = """
DROP TABLE IF EXISTS "artist";
"""
time_table_drop = """
DROP TABLE IF EXISTS "time";
"""

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events" (
    "id" double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
    "artist"  character varying (255) NOT NULL,
    "auth" character varying (11) NOT NULL,
    "gender" character varying (1) NOT NULL,
    "itemiInSession" numeric NOT NULL,
    "lastName" character varying (255) NOT NULL,
    "length" double precision NOT NULL,
    "level" character varying (4) NOT NULL,
    "location" character varying (255) NOT NULL,
    "method" character varying (4) NOT NULL,
    "registration" double precision NOT NULL,
    "sessionId" numeric NOT NULL,
    "song" character varying (255) NOT NULL,
    "status" numeric(3,0) NOT NULL,
    "ts" character varying (255) NOT NULL,
    "userAgent" character varying (255) NOT NULL,
    "userId" numeric NOT NULL,
);
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
