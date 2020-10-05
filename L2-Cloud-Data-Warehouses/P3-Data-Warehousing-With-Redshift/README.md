
# Introduction - Sparkify Analytics
In this project, we are doing data modelling and building an ETL pipeline on Amazon Redshift for a music streaming startup called Sparkify. The project consists of 3 main parts:

 - Managing Redshift cluster through Python boto3 library as infrastructure as code (IaC)
 - Relational data modelling with Redshift using star schema.
 - Creating an ETL pipeline from data on S3 into staging tables in Redshift and finally analytics tables in Redshift.

# Context
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Datasets
In this project, we are working with 2 datasets that reside in S3.

**Song dataset**:
The files are partitioned by the first three letters of each song's track ID:

    song_data/A/B/C/TRABCEI128F424C983.json


Each file contains metadata about a song and the artist of that song in JSON format. A sample file:   

    {"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}

**Log Dataset**: 
The log files in the dataset are partitioned by year and month:

    sdsdsd log_data/2018/11/2018-11-12-events.json
Users activity log in JSON format. A sample file:

    {"artist":"Girl Talk","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":8,"lastName":"Summers","length":160.15628,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"Once again","status":200,"ts":1541107734796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}

# Database Schema Design
In this project, we have staging tables and fact/dimension tables for the star schema design. Users log and song data are loaded from S3 into staging tables and then inserted into fact/dimension tables,

Star schema is used here and optimized for the analysis of song plays. There is one main Fact table that focuses on song plate metrics and 4 Dimension tables associated with users, songs, artists and time.
## Song Plays Table
This is the Fact table in the star schema design.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `songplay_id` | `bigint identity(0, 1)` | The primary key of the table. | 
| `start_time` | `timestamp NOT NULL REFERENCES time(start_time)` | The unix timestamp of the activity in ms. |
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
| `year` | `smallint` | Year the song is released. |
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
| `start_time` | `timestamp PRIMARY KEY` | The unix timestamp in ms.|
| `hour` | `smallint NOT NULL` | Corresponding hour |
| `day` | `smallint NOT NULL` | Corresponding day |
| `week` | `smallint NOT NULL` | Corresponding week |
| `month` | `smallint NOT NULL` | Corresponding month |
| `year` | `smallint NOT NULL` | Corresponding year |
| `weekday` | `smallint NOT NULL` | Corresponding weekday |

# Project Structure
The project consists of following files:

 1. **manage_dwh.py**:  creates/deletes Redshift cluster programmatically using boto3 library (**IaC**).
 2. **sql_queries.py** : contains SQL queries for ETL job such as COPY statements for staging tables, CREATE and INSERT statements for fact and dimension tables.
 3. **create_tables.py** : creates the tables in the database based on the star schema defined above.
 4. **etl.py** : performs ETL job, copies user log & songs data from S3 buckets into stating tables and then inserts data from staging tables into the fact and dimension tables.
 5. **dwh.cfg** : The configuration file for AWS, S3 buckets, Redshift cluster properties and IAM roles.
 
# Running the scripts
After cloning the repository, please follow these steps below to execute the project:

 1. Put IAM user credentials ('access key id' and 'secret access key') into the configuration file '**dwh.cfg**'. IAM user should have programmatic access and appropriate access credentials.
 2. If needed, please modify Redshift cluster properties such node type, number of node and etc. in the configuration file.
 3. Create the Redshift cluster, this script waits until the cluster status becomes available. The script is verbose and provides helpful logs, please make sure that Redshift cluster is created and available for use.
     
     `python manage_dwh.py create`
 4. Create staging tables and fact & dimension tables in Redshift using psycopg2 module.
    
    `python create_tables.py`
 5. Run the ETL job. This script load the data from S3 buckets into stating tables in Redshift and finally inserts relevant data into fact & dimension tables for analytics.
    
    `python etl.py`
6. After you are done, delete the Redshift cluster. Again, the script is verbose and provide helpful logs. Please make sure that the cluster is deleted successfully.
    
    `python manage_dwh.py delete`
