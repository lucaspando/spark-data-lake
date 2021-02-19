import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULTS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULTS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data", "*", "*", "*", "*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = os.path.join(output_data, "songs"), mode = "overwrite")

    # extract columns to create artists table
    artists_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_columns = [ " ".join([column, "as", column.replace("artist_", "")]) if column not in ("artist_id") else column for column in artists_columns]
    artists_table = df.selectExpr(*artists_columns).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(path = os.path.join(output_data, "artists"), mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data", "*", "*", "*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table  
    user_columns = ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level']
    users_table = df.selectExpr(user_columns).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(path = os.path.join(output_data, "users"), mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create  column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), DateType())
    df = df.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.selectExpr(["timestamp as start_time"])
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = os.path.join(output_data, "time"), mode = "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df \
        .join(song_df, song_df.title == df.song, 'inner') \
        .orderBy('ts') \
        .selectExpr(["timestamp as start_time",
                    "userId as user_id", "level",
                    "song_id",
                    "artist_id", 
                    "sessionId as session_id", 
                    "location", 
                    "userAgent as user_agent", 
                    "year(timestamp) as year", 
                    "month(timestamp) as month"]) \
        .withColumn('songplay_id', monotonically_increasing_id())
        

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path = os.path.join(output_data, "songplays"), mode = "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-de/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
