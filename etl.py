import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS","AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS","AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """
    This function creates a spark session. 
    
    NO INPUTS
    
    OUTPUT: returns a variable referencing the created session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    
    This function processes and creates the tables for the song data. 
    This data includes song and artist's names, ids, duration of the songs and location of the artists
    
    INPUTS 
    
    1- spark: the spark session currently running
    2- input_data: the S3 bucket where the original data is stored.
    3- output_data: the S3 bucket where the data will be stored after processing
    
    NO OUTPUTS
    
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView('song_log')
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id, title, artist_id, year, duration
                            FROM song_log
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
                .partitionBy("year","artist_id")\
                .mode("overwrite")\
                .parquet("{}songs/songs_table.parquet".format(output_data))
    

    # extract columns to create artists table
    artists_table = spark.sql(""" 
                    SELECT DISTINCT artist_id, 
                                    artist_name,
                                    artist_location,
                                    artist_latitude,
                                    artist_longitude 
                    FROM song_log""")
    
    # write artists table to parquet files
    artists_table.write\
                .mode("overwrite")\
                .parquet("{}artists/artist_table.parquet".format(output_data))


def process_log_data(spark, input_data, output_data):
    
    """
    
    This function processes and creates the log data. 
    This data includes user's infos such as names, id, gender, location as well as 
    events information for example sessionId and start time.
    
    INPUTS
    
    1- spark: the spark session currently running
    2- input_data: the S3 bucket where the original data is stored.
    3- output_data: the S3 bucket where the data will be stored after processing
    
    NO OUTPUTS
    
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df=df.filter(df['page']=='NextSong')
    
    df.createOrReplaceTempView('logs')
    

    # extract columns for users table    
    users_table = spark.sql(""" 
                SELECT DISTINCT userId as user_id, 
                        firstName as first_name, 
                        lastName as last_name, 
                        gender,
                        level
                FROM logs
                    """)
    
    # write users table to parquet files
    users_table.write\
                .mode("overwrite")\
                .parquet("{}users/users_table.parquet".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000), TimestampType())
    df = df.withColumn('ts',get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table 
    
    df.createOrReplaceTempView("event_log")
    
    time_table=spark.sql(""" 
                            SELECT ts as start_time,
                                hour(ts) as hour,
                                day(ts) as day,
                                weekofyear(ts) as weekday,
                                month(ts) as month,
                                year(ts) as year,
                                dayofweek(ts) as dayofweek
                            FROM event_log
                        """)
     
    
    # write time table to parquet files partitioned by year and month
    time_table.write\
                .partitionBy('year','month')\
                .mode('overwrite')\
                .parquet("{}time/time_table.parquet".format(output_data))

    # read in song data to use for songplays table
    
    song_df = spark.read.parquet(output_data+'songs/')
    song_df.createOrReplaceTempView('songs_table')
    time_table.createOrReplaceTempView('time_table')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                    SELECT e.ts as start_time,
                                      e.userId as user_id,
                                      e.level,
                                      s.song_id,
                                      s.artist_id,
                                      e.sessionId as session_id,
                                      e.location,
                                      e.userAgent as user_agent,
                                      t.year,
                                      t.month
                                    FROM event_log e
                                    JOIN songs_table s 
                                    ON s.title = e.song
                                    JOIN time_table t ON t.start_time= e.ts  
                                """)
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write\
                    .partitionBy('year','month')\
                    .mode('overwrite')\
                    .parquet('{}songplay/songplay_table.parquet'.format(output_data))


def main():
    
    """
    This is the main function, used to call other functions and assign values to the initial variables.
    
    NO INPUTS
    
    NO OUTPUTS
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacityvitorsparkfy/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
