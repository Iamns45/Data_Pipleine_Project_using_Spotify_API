CREATE or REPLACE database api_project

CREATE or REPLACE storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN='arn:aws:iam::471112560860:role/snowflake_s3_connection'
    STORAGE_ALLOWED_LOCATIONS = ('s3://ns-etl-project')
    COMMENT='storage integration with s3 and snowflake'

DESC integration s3_init

CREATE or REPLACE file format csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header=1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;  

CREATE or REPLACE stage spotify_stage
    URL='s3://ns-etl-project/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat

LIST @spotify_stage

LIST @spotify_stage/songs

CREATE or REPLACE table album(
    album_id STRING,
    name STRING,
    release_date DATE,
    total_tracks INTEGER,
    url STRING
)


CREATE or REPLACE table artist(
    artist_id STRING,
    name STRING,
    url STRING
)

CREATE or REPLACE table songs(
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
)


COPY INTO songs
FROM @spotify_stage/songs_data/songs_transformed_2024-03-17

COPY INTO artist
FROM @spotify_stage/artist_data/artist_transformed_2024-03-17

COPY INTO album
FROM @spotify_stage/album_data/album_transformed_2024-03
ON_ERROR = 'CONTINUE'

SELECT * FROM album

SELECT * FROM songs

SELECT * FROM artist


CREATE or REPLACE SCHEMA pipe

CREATE or REPLACE  pipe pipe.songs_table_pipe
auto_ingest = TRUE
AS
COPY INTO api_project.public.songs
FROM @api_project.public.spotify_stage/songs_data

CREATE or REPLACE  pipe pipe.artist_table_pipe
auto_ingest = TRUE
AS
COPY INTO api_project.public.artist
FROM @api_project.public.spotify_stage/artist_data

CREATE or REPLACE  pipe pipe.album_table_pipe
auto_ingest = TRUE
AS
COPY INTO api_project.public.album
FROM @api_project.public.spotify_stage/album_data

DESC pipe pipe.songs_table_pipe

DESC pipe pipe.album_table_pipe


DESC pipe pipe.artist_table_pipe

SELECT COUNT(*) FROM api_project.public.ALBUM

