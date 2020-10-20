
# Data Lake for Sparkify Analytics
In this project, we are building a data lake on AWS S3 using AWS Elastic MapReduce (EMR) service and Apache Spark.
The project consists of 2 main parts:

 - Creating an EMR cluster to perform ETL.
 - Running ETL pipeline on EMR (or locally) using Spark and building data lake on S3 with analytics tables.

# Context
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Datasets
In this project, we are working with 2 datasets that reside in S3.
 - Song data: `s3://udacity-dend/song_data`
 - Log data: `s3://udacity-dend/log_data`

**Song dataset**:
The files are partitioned by the first three letters of each song's track ID:

    song_data/A/B/C/TRABCEI128F424C983.json


Each file contains metadata about a song and the artist of that song in JSON format. A sample file:   

    {"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}

**Log Dataset**: 
The log files in the dataset are partitioned by year and month:

    log_data/2018/11/2018-11-12-events.json
Users activity log in JSON format. A sample file:

    {"artist":"Girl Talk","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":8,"lastName":"Summers","length":160.15628,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"Once again","status":200,"ts":1541107734796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}

# Schema Design for Analytics Tables
Star schema is used here and optimized for the analysis of song plays. There is one main Fact table that focuses on song plate metrics and 4 Dimension tables associated with users, songs, artists and time.

Users log and song dataset are loaded from S3 into EMR cluster, processed in memory using Spark and then the resulting analytics table are writted back to S3 as parquet files for later use.

## Song Plays Table
This is the Fact table in the star schema design.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `songplay_id` | `long` | The primary key for songplays. | 
| `start_time` | `long` | The unix timestamp of the activity in ms. |
| `user_id` | `string` | The id of the user on the app. |
| `level` | `string` | The subscription level of the user. |
| `song_id` | `string` | The id of the song. |
| `artist_id` | `string` | The id of the artist whose song is played. |
| `session_id` | `long` | The session id of the user on the app. |
| `location` | `string` | The location where the song is played. |
| `user_agent` | `string` | Agent used to access the app. |

## Users Table
This is a dimension table about the users.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `user_id` | `string` | The id of the user on the app. |
| `first_name` | `string` | First name of the user. |
| `last_name` | `string` | Last name of the user. |
| `gender` | `string` | Gender of the user. |
| `level` | `string` | The subscription level of the user. |

## Songs table
This is a dimension table about the songs.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `song_id` | `string` | The id of a song. | 
| `title` | `string` | The title of the song. |
| `artist_id` | `string` | The id of the artist that the song belongs to. |
| `year` | `integer` | Year the song is released. |
| `duration` | `double` | The duration of the song in seconds. |


## Artists table
This is a dimension table about the artists.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist_id` | `string` | The id of an artist. |
| `name` | `string` | The name of the artist. |
| `location` | `string` | The location of the artist. |
| `latitude` | `double` | The latitude of the location. |
| `longitude` | `double` | The longitude of the location. |

## Time table
This is a dimension table about the timestamps.
| Column | Type | Description |
| ------ | ---- | ----------- |
| `start_time` | `long` | The unix timestamp in ms.|
| `hour` | `integer` | Corresponding hour |
| `day` | `integer` | Corresponding day |
| `week` | `integer` | Corresponding week |
| `month` | `integer` | Corresponding month |
| `year` | `integer` | Corresponding year |
| `weekday` | `integer` | Corresponding weekday |

# Project Structure
The project consists of following files:

**dl.cfg**:  The configuration file for AWS access credentials & output S3 bucket.

**etl.py** : performs ETL job:
  - Reads user log & songs data from S3 buckets.
  - Transforms them into the fact and dimension tables using Spark.
  - Writes the fact and dimension tables into S3 as parquet files.

**data_explore_s3.ipynb**:  A simple notebook to explore files in S3 buckets.

**check_analytics_tables.ipynb**:  A notebook to check songsplays table on the data lake build after the ETL.
 
# Running the scripts
After cloning the repository, please follow these steps below to execute the project:

## Local Mode
 1. Put AWS access credentials ('access key id' and 'secret access key') into the configuration file '**dl.cfg**'.
 2. Run the ETL script as shown below, it creates a S3 bucket based on the current timestamp using boto3 library and provided AWS access credentials. The bucket is created on on region specified in the config file.
     
     `python etl.py --local`
 4. or if you already have an existing bucket, run the ETL script as shown below.
    
    `python etl.py --local --bucket "<your_S3_bucket_name>" `

## EMR Cluster Mode
 1. Put AWS access credentials ('access key id' and 'secret access key') into the configuration file '**dl.cfg**'.
 2. Create a S3 bucket to store analytics tables. You can put this bucket name in the configuration file '**dl.cfg**' or pass it as an argument when running the script.
 3. Create an EMR cluster with Spark. I setup the cluster with following HW configurations:
 - 1 Master and 3 Core nodes
 - Chose m4.large (2 vCore, 8 GiB memory) instance type for each node
 4. Send '**dl.cfg**' and '**etl.py**' to master node through scp.
 
    `scp -i <path_to_your_pem_file> dl.cfg etl.py hadoop@<emr_master_public_dns_name>:/home/hadoop/.`
 5. or instead of step #4, you can clone the repository directly on EMR master node.
 
 6. Connect to your EMR master node through ssh:
 
    `ssh -i <path_to_your_pem_file> hadoop@<emr_master_public_dns_name>`
 
 7. Submit the Spark job as shown below if you set your S3 bucket name in the config file:
 
    `spark-submit --master yarn ./etl.py`
 8. or submit the Spark job as shown below if you want to specify S3 bucket name for data lake as an argument:
 
    `spark-submit --master yarn ./etl.py --bucket "<your_S3_bucket_name>" `
