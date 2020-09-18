# Introduction - Sparkify Analytics
In this project, we are doing data modelling and building an ETL pipeline for a music streaming startup called Sparkify. The project consists of 2 main parts:

 - Relational data modelling with Postgres using star schema.
 - Creating an ETL pipeline in Python.

# Context
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Datasets
**Song dataset**: Each file contains metadata about a song and the artist of that song in JSON format. A sample file:   

    {"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}

**Log Dataset**: Users activity log in JSON format. A sample file:

    {"artist":"Girl Talk","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":8,"lastName":"Summers","length":160.15628,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"Once again","status":200,"ts":1541107734796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}

# Database Schema Design
Star schema is used here and optimized for the analysis of song plays. There is one main Fact table that focuses on song plate metrics and 4 Dimension tables associated with users, songs, artists and time.
## Song Plays Table
This is the Fact table in the star schema design.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `songplay_id` | `bigserial primary key` | The primary key of the table. | 
| `start_time` | `bigint NOT NULL REFERENCES time(start_time)` | The unix timestamp of the activity in ms. |
| `user_id` | `int NOT NULL REFERENCES users(user_id)` | The id of the user on the app. |
| `level` | `varchar NOT NULL` | The subscription level of the user. |
| `song_id` | `varchar REFERENCES songs(song_id)` | The id of the song. |
| `artist_id` | `varchar REFERENCES artists(artist_id)` | The id of the artist whose song is played. |
| `session_id` | `integer NOT NULL` | The session id of the user on the app. |
| `location` | `varchar` | The location where the song is played. |
| `user_agent` | `varchar` | Agent used to access the app. |

## Users Table
This is a dimension table about the users.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `user_id` | `int PRIMARY KEY` | The id of the user on the app. |
| `first_name` | `varchar NOT NULL` | First name of the user. |
| `last_name` | `varchar NOT NULL` | Last name of the user. |
| `gender` | `varchar` | Gender of the user. |
| `level` | `varchar NOT NULL` | The subscription level of the user. |

## Songs table
This is a dimension table about the songs.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `song_id` | `varchar PRIMARY KEY` | The id of a song. | 
| `title` | `varchar NOT NULL` | The title of the song. |
| `artist_id` | `varchar NOT NULL` | The id of the artist that the song belongs to. |
| `year` | `int` | Year the song is released. |
| `duration` | `numeric` | The duration of the song in seconds. |


## Artists table
This is a dimension table about the artists.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist_id` | `varchar PRIMARY KEY` | The id of an artist. |
| `name` | `varchar NOT NULL` | The name of the artist. |
| `location` | `varchar` | The location of the artist. |
| `latitude` | `numeric` | The latitude of the location. |
| `longitude` | `numeric` | The longitude of the location. |

## Time table
This is a dimension table about the timestamps.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `start_time` | `bigint PRIMARY KEY` | The unix timestamp in ms.|
| `hour` | `smallint NOT NULL` | Corresponding hour |
| `day` | `smallint NOT NULL` | Corresponding day |
| `week` | `smallint NOT NULL` | Corresponding week |
| `month` | `smallint NOT NULL` | Corresponding month |
| `year` | `int NOT NULL` | Corresponding year |
| `weekday` | `smallint NOT NULL` | Corresponding weekday |

# Project Structure
The project consists of following files:

 1. **sql_queries.py** : contains all SQL queries such as CREATE and INSERT statements for fact and dimension tables.
 2. **create_tables.py** : creates the tables in the database based on the star schema defined above.
 3. **etl.py** : performs ETL job, parses raw files and load the structured data into the fact and dimension tables.
 4. **data** : The directory that contains song and user log files.

# Running the scripts
As explained in the introduction, the project consist of 2 main parts.

 1. Creates the database and fact & dimension tables in Postgres using psycopg2 module:
    `python create_tables.py`
 2. Run the ETL job to process song and user log files in JSON format:
    `python etl.py`
