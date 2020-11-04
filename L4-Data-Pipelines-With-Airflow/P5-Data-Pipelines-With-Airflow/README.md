# Data Pipeline for Sparkify Analytics
In this project, we are building a data pipeline using Apache Airflow to automate ETL jobs and integrate data quality checks.
The project consists of 2 main parts:

 - Creating a Redshift cluster and seting up AWS and Redshift connections on Airflow UI.
 - Running data pipeline 'sparkify-dag' which performs ETL jobs from S3 to Redshift with integrated data quality checks.

# Context
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

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

# Sparkify DAG
The dag is designed to perform the following steps:
- Copy song and user log data from S3 to Redshift into stating tables.
- Transform data from staging tables and insert into the songplays Fact table.
- Transform data from staging tables and insert into the dimention tables 'users', 'songs', 'artists', 'time'.
- Perform data quality check on the fact and dimention tables and raise an error if needed

The dag is configured with the following properties:
- No dependency on past runs
- On failure, tasks are retried 3 times
- Retries happen every 5 minutes
- No emails are sent on retries
- Catchup is turned off

Task dependencies as visualized on Airflow UI:

![image](https://github.com/mpoyraz/Udacity-Data-Engineering-Nanodegree/blob/master/L4-Data-Pipelines-With-Airflow/P5-Data-Pipelines-With-Airflow/images/sparkify-dag.png)

# Project Structure
The data pipeline is implemented with Airflow and has the following structure:
```bash
airflow
├── dags
├── ├── sparkify-dag.py
├── plugins
├── ├── __init__.py
├── ├── operators
├── ├── ├── __init__.py
├── ├── ├── stage_redshift.py
├── ├── ├── load_fact.py
├── ├── ├── load_dimension.py
├── ├── ├── data_quality.py
├── ├── helpers
├── ├── ├── __init__.py
├── ├── ├── sql_queries.py
```

# Data Quality Operator
A custom Airflow operator with a Redshift (Postgres) hook is developed to perform data quality checks after ETL jobs are finished.
The operator accepts list of Redshift tables and perform 2 types of checks per table:
1. First, the operator checks number of records in each table.
    - If a table has no rows, logs the issue and raises a ValueError.
    - If all tables passed this initial data quality check, the operator moves on the 2nd step.
2. The operator check if a table has any primary keys columns (can be composite as well).
    - If true, further checks if primary key columns contains any nulls. If any null value present in the primary key column, data quality check fails.
    - If false, data quality check is completed successfully.

Data quality check logs from a test run:
TODO: add the data quality logs later

# Running the Data Pipeline
Perform the following steps to run the data pipeline:
1. Clone the repository where Airflow is installed & available.
2. Start Airflow UI and go to Admin -> Connections page.
    - Add a Amazon WebServices connection named `aws_credentials` using AWS Access key ID as login and Secret access key as password.
    - Add a Postgress connection named `redshift` and fill up host, schema, login, password and port fields based on Redshift cluster properties.
3. The sparkify dag is scheduled to run hourly intervals, trigger it manually and monitor its progress as it performs the ETL jobs and then data quality check.
