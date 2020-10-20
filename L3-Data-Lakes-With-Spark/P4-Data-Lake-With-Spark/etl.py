import argparse
import configparser
import os
from datetime import datetime
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, from_unixtime, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Parse the configuration file
config = configparser.ConfigParser()
config.read('dl.cfg')

# Set the environment variables for AWS access
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# Log prefix for ETL job
log_prefix = "ETL SPARKIFY"

# Estimated number of partitions based on ~6 CPU cores and dataset size.
num_cpu = 6
num_partitions = 2 * num_cpu

def create_spark_session():
    """ Creates Spark Session object with appropiate configurations.
    
    Returns:
    spark: Spark Session object
    """
    print("{}: creating Spark Session...".format(log_prefix))
    
    spark = SparkSession \
        .builder \
        .appName("ETL Sparkify") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    # Speed up the file writing into S3
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    # Set the log level
    spark.sparkContext.setLogLevel('INFO')
    # Set dataframe shuffle partitions
    spark.conf.set("spark.sql.shuffle.partitions", num_partitions)
    
    print("{}: Spark Session is created and ready for use".format(log_prefix))
    return spark


def process_song_data(spark, input_data, output_data):
    """ Reads song dataset from S3 and transfroms it into songs and artists tables,
        songs and artist tables are written back to S3.
    
    Args:
    spark : Spark Session object
    input_data (str): input S3 bucket path
    output_data (str): output S3 bucket path
    """
    # Get filepath to song data file
    song_data_path = os.path.join(input_data, 'song_data/*/*/*/*.json')
    print("{}: start processing {}".format(log_prefix, song_data_path))
    
    # Define the schema for songs data files
    schema_song = StructType([StructField('artist_id', StringType(), True),
                              StructField('artist_latitude', DoubleType(), True),
                              StructField('artist_longitude', DoubleType(), True),
                              StructField('artist_location', StringType(), True),
                              StructField('artist_name', StringType(), True),
                              StructField('duration', DoubleType(), True),
                              StructField('num_songs', LongType(), True),
                              StructField('song_id', StringType(), True),
                              StructField('title', StringType(), True),
                              StructField('year', LongType(), True)])
    
    # Read song data file
    df_song = spark.read.json(song_data_path, schema = schema_song)
    for i in range(100):
        print("df_song before: ", df_song.rdd.getNumPartitions())
    
    # Repartition
    df_song = df_song.repartition(num_partitions)
    
    # Persist song dataframe for reuse
    df_song.persist(StorageLevel.MEMORY_AND_DISK)
    
    # Extract columns to create songs table
    songs_table = df_song.select('song_id','title','artist_id','year','duration').dropDuplicates(['song_id'])
    
    # Write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data, 'songs'), mode='overwrite')
    print("{}: songs table is written into S3".format(log_prefix))
    
    # Extract columns to create artists table
    artists_table = df_song.select('artist_id',
                                   col('artist_name').alias('name'), \
                                   col('artist_location').alias('location'), \
                                   col('artist_latitude').alias('latitude'), \
                                   col('artist_longitude').alias('longitude') \
                                  ).dropDuplicates(['artist_id'])
    
    # Write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))
    print("{}: artists table is written into S3".format(log_prefix))
    
    # Unpersist song dataframe
    df_song.unpersist()
    print("{}: song dataset processing is finished".format(log_prefix))


def process_log_data(spark, input_data, output_data):
    """ Reads log dataset from S3 and transfroms it into users, time & songplays tables,
        finally these tables are written back to S3.
    
    Args:
    spark : Spark Session object
    input_data (str): input S3 bucket path
    output_data (str): output S3 bucket path
    """
    # Get filepath to log data file
    log_data_path = os.path.join(input_data, 'log-data/*/*/*.json')
    print("{}: start processing {}".format(log_prefix, log_data_path))
    
    # Define the schema for log data files
    schema_log = StructType([StructField('artist', StringType(), True),
                             StructField('auth', StringType(), True),
                             StructField('firstName', StringType(), True),
                             StructField('gender', StringType(), True),
                             StructField('itemInSession', LongType(), True),
                             StructField('lastName', StringType(), True),
                             StructField('length', DoubleType(), True),
                             StructField('level', StringType(), True),
                             StructField('location', StringType(), True),
                             StructField('method', StringType(), True),
                             StructField('page', StringType(), True),
                             StructField('registration', StringType(), True),
                             StructField('sessionId', LongType(), True),
                             StructField('song', StringType(), True),
                             StructField('status', LongType(), True),
                             StructField('ts', LongType(), True),
                             StructField('userAgent', StringType(), True),
                             StructField('userId', StringType(), True)])
    
    # Read log data file
    df_log = spark.read.json(log_data_path, schema = schema_log)
    
    # Repartition
    df_log = df_log.repartition(num_partitions)
    
    # Persist logs dataframe for reuse
    df_log.persist(StorageLevel.MEMORY_AND_DISK)
    
    # Filter by actions for song plays
    df_log_nextSong = df_log.filter(df_log.userId.isNotNull()).filter(df_log.page == 'NextSong')
    
    # Extract columns for users table
    users_latest_state = df_log_nextSong.groupBy('userId').max('ts') \
                         .select("userId", col("max(ts)").alias("ts"))
 
    users_table = df_log_nextSong.join(users_latest_state, on = ['userId','ts']) \
                             .select(col('userId').alias('user_id'), \
                                     col('firstName').alias('first_name'), \
                                     col('lastName').alias('last_name'), \
                                     'gender', 'level')
   
    # Write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))
    print("{}: users table is written into S3".format(log_prefix))
    
    # Create datetime column from original timestamp column
    convert_ms_to_s = udf(lambda x: x//1000, LongType())
    df_timestamp = df_log_nextSong.select(col('ts').alias('start_time')).dropDuplicates()
    df_timestamp = df_timestamp.withColumn("datetime", from_unixtime(convert_ms_to_s(df_timestamp.start_time)))
    
    # Extract columns to create time table
    time_table = df_timestamp.withColumn("hour", hour("datetime")) \
                         .withColumn("day", dayofmonth("datetime")) \
                         .withColumn("week", weekofyear("datetime")) \
                         .withColumn("month", month("datetime")) \
                         .withColumn("year", year("datetime")) \
                         .withColumn("weekday", dayofweek("datetime")) \
                         .drop('datetime')
    
    # Write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'time'))
    print("{}: time table is written into S3".format(log_prefix))
    
    # Read in songs & artists tables to use for songplays table
    songs_table = spark.read.parquet(os.path.join(output_data, 'songs'))
    artists_table = spark.read.parquet(os.path.join(output_data, 'artists'))

    # Extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_log_nextSong.join(songs_table, df_log_nextSong.song == songs_table.title) \
                                 .join(artists_table, df_log_nextSong.artist == artists_table.name) \
                                 .join(time_table, df_log_nextSong.ts == time_table.start_time) \
                                 .select(df_log_nextSong.ts.alias('start_time'), \
                                         df_log_nextSong.userId.alias('user_id'), \
                                         df_log_nextSong.level, \
                                         songs_table.song_id, \
                                         artists_table.artist_id, \
                                         df_log_nextSong.sessionId.alias('session_id'), \
                                         df_log_nextSong.location, \
                                         df_log_nextSong.userAgent.alias('user_agent'), \
                                         time_table.year, \
                                         time_table.month ) \
                                  .withColumn('songplay_id', row_number().over(Window().orderBy('song_id')))

    # Write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'songplays'))
    
    # Unpersist song dataframe
    df_log.unpersist()
    print("{}: logs dataset processing is finished".format(log_prefix))

def generate_s3_bucket_name(prefix = "sparkify-analytics"):
    """ Creates a unique S3 bucket name with given prefix and current timestamp.
    
    Args:
    prefix (str) : Spark Session object
    
    Returns:
    bucket_name (str): S3 compatabile bucket name
    """
    dt = datetime.now()
    dt_formatted = "{}-{:02d}-{:02d}-{:02d}-{:02d}-{:02d}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
    bucket_name = "{}-{}".format(prefix, dt_formatted)
    print("The output bucket name for S3: {}".format(bucket_name))
    return bucket_name
    
def create_s3_bucket(bucket_name):
    """ Creates S3 bucket with the given name using boto3 library.
        The bucket is created on AWS region provided in the config file.
    
    Args:
    bucket_name (str) : S3 bucket name to create
    """
    import boto3
    s3 = boto3.resource('s3',
                        region_name = config['AWS']['REGION'],
                        aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
                        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
                       )
    
    try:
        response = s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': config['AWS']['REGION'],
            },
        )
    except Exception as e:
        if 'BucketAlreadyOwnedByYou' in str(e):
            print("The bucket '{}' is already own by you".format(bucket_name))
        elif 'BucketAlreadyExists' in str(e):
            print("The bucket '{}' already exists")
        else:
            raise(e)
    else:
        print("The bucket name '{}' is successfully created on S3".format(bucket_name))
    
def main(args):
    """ Performs ETL on song and user logs datasets to create analytics tables on S3.
    
    Args:
    args.bucket (str) : Existing S3 bucket name to store analytics tables
    args.local (boolean) : Local testing, creates a S3 bucket if not specified
    """
    # S3 bucket name for input data
    input_data = "s3a://udacity-dend/"
    
    # S3 bucket name for output data
    if args.bucket is None:
        print("{}: S3 bucket name is not specified as command-line arguments".format(log_prefix))
        if args.local:
            print("{}: creating an S3 bucket using boto3 library".format(log_prefix))
            output_bucket = generate_s3_bucket_name()
            # create the S3 bucket to store fact/dimentional tables
            try:
                create_s3_bucket(bucket_name=output_bucket)
            except Exception as e:
                print(e)
                return
        else:
            print("{}: will try to read S3 bucket name from config file".format(log_prefix))
            output_bucket = config['AWS']['S3_BUCKET_NAME']
    else:
        output_bucket = args.bucket
    
    print("{}: S3 bucket name for output tables: {}".format(log_prefix, output_bucket))
    output_data = "s3a://{}/".format(output_bucket)
    
    # create the spark session
    spark = create_spark_session()
    
    # process the song and user log files on S3
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    print("{}: the ETL job is finished".format(log_prefix))
    spark.stop()

if __name__ == "__main__":
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Sparkify ETL")
    
    parser.add_argument("--bucket", type=str, help="S3 bucket name to store output tables")
    parser.add_argument("--local", help="Local testing, creates a S3 bucket if not specified", action="store_true")
    args = parser.parse_args()
    
    main(args)
