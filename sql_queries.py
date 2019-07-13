import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplays;"
user_table_drop = "DROP TABLE IF EXISTS dimUsers;"
song_table_drop = "DROP TABLE IF EXISTS dimSongs;"
artist_table_drop = "DROP TABLE IF EXISTS dimArtists;"
time_table_drop = "DROP TABLE IF EXISTS dimTime;"

# CREATE TABLES

#Since this is a staging table for the data contained in the JSON files on S3, we don't put any constraints on the columns in order to load all the data
#the row data type is chosen based on how the data is stored in the JSON file. For instance, userId i stored as an character string and contains null values.
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist          TEXT,	
    auth            TEXT,	
    firstName       TEXT,	
    gender          TEXT,	
    itemInSession   INTEGER,	
    lastName        TEXT,	
    length          NUMERIC,	
    level           TEXT,	
    location        TEXT,	
    method          TEXT,	
    page            TEXT,	
    registration    NUMERIC,	
    sessionId       INTEGER,	
    song            TEXT,	
    status          INTEGER,	
    ts              BIGINT,	
    userAgent       TEXT,	
    userId          TEXT
);
""")

#Since this is a staging table for the data contained in the JSON files on S3, we don't put any constraints on the columns in order to load all the data
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs           INTEGER, 
    artist_id           TEXT, 
    artist_latitude     TEXT, 
    artist_longitude    TEXT, 
    artist_location     TEXT, 
    artist_name         TEXT, 
    song_id             TEXT, 
    title               TEXT, 
    duration            NUMERIC, 
    year                INTEGER
);
""")


#songplay_id is an artificial key that gets incremented everytime a new row is inserted into the table
#the 4 foreign keys (start_time, user_id, song_id and artist_id) form a unique combination  
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS factSongplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
    start_time BIGINT NOT NULL REFERENCES time(start_time),
    user_id INT NOT NULL REFERENCES users(user_id), 
    level VARCHAR, 
    song_id VARCHAR REFERENCES songs(song_id), 
    artist_id VARCHAR REFERENCES artists(artist_id), 
    session_id INT, 
    location VARCHAR, 
    user_agent VARCHAR,
    
    UNIQUE (start_time, user_id, song_id, artist_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dimUsers(
    user_id INT PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR,
    gender VARCHAR, 
    level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dimSongs(
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR, 
    artist_id VARCHAR NOT NULL, 
    year INT, 
    duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dimArtists( 
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR, 
    location VARCHAR, 
    lattitude VARCHAR, 
    longitude VARCHAR
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dimTime( 
    start_time BIGINT PRIMARY KEY,
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2' FORMAT AS JSON {};
""").format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2' FORMAT AS JSON 'auto';
""").format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))

# FINAL TABLES


#The query below loads the song play data.
#we perform a LEFT JOIN since a number of rows in the staging events table have songs that don't appear in the song staging table and filter on page = 'NextSong'
#Furthermore, since the column userId contains null values, we filter out rows containing null values and convert userId into INTEGERs
songplay_table_insert = ("""
INSERT INTO factSongplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    stg_e.ts,
    stg_e.userId::INTEGER,      
    stg_e.level,              
    stg_s.song_id,
    stg_s.artist_id,
    stg_e.sessionId,       
    stg_e.location,    
    stg_e.userAgent
FROM staging_events AS stg_e
LEFT JOIN staging_songs AS stg_s 
ON (stg_e.artist =  stg_s.artist_name) 
AND (stg_e.song =  stg_s.title)
WHERE stg_e.userId !=''
AND stg_e.page = 'NextSong';

""")

# We aggregate the data by uniquer user_id before loading it into the dimension dable
# since we load data with an empty user_id into the staging area, we must filter out all entries with empty user_id before loading them into the table.
user_table_insert = ("""
INSERT INTO dimUsers(user_id, first_name, last_name, gender, level)
SELECT     
    DISTINCT stg.userId::INTEGER,
    stg.firstName,
    stg.lastName,
    stg.gender,
    stg.level
FROM staging_events AS stg
WHERE stg.userId !='';
""")

#We aggregate the data by unique song_id. No column requires any conversion before being loaded into the dimension table
song_table_insert = ("""
INSERT INTO dimSongs(song_id, title, artist_id, year, duration)
SELECT
    DISTINCT stg.song_id,
    stg.title, 
    stg.artist_id, 
    stg.year, 
    stg.duration
FROM staging_songs AS stg;
""")

#We aggregate the data by unique artist_id. No column requires any conversion before being loaded into the dimension table
artist_table_insert = ("""
INSERT INTO dimArtists(artist_id, name, location, lattitude, longitude)
SELECT     
    DISTINCT stg.artist_id, 
    stg.artist_name, 
    stg.artist_location, 
    stg.artist_latitude, 
    stg.artist_longitude
FROM staging_songs AS stg;
""")


# Redshift does not support directly converting a timestamp to date directly and must be converted manually as a reuslt.
# We aggregate the data by unique ts value.
# For the day of the week column, since Sunday=0 by default, we subtract 1 from the number we get to get Monday=0 (to be consistent with the data representation from project 1).  

time_table_insert = ("""
INSERT INTO dimTime(start_time, hour, day, week, month, year, weekday)

SELECT
        DISTINCT ts,
        extract(hr from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
        extract(day from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
        extract(w from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
        extract(mon from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
        extract(y from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second ')),
        extract(dow from (TIMESTAMP WITH TIME ZONE 'epoch' + ts/1000 * INTERVAL '1 Second '))
FROM staging_events;
""")

# QUERY LISTS

#we switched the table creation order to allow for the foreign key references to be added to our dimensional model
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
