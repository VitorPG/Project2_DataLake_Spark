# Sparkfy Project (Spark + S3)

This project was used to create a database for Sparkfy. Due to the large amount of data stored in a AWS's S3 bucket, 
it is processed using Spark. 

## File Content

This project runs on a simple file: etl.py. Which makes it easy to run on a spark cluster. 
For safety reasons, a dl.cfg file was created to setup access to AWS. 

1. README.md: this file. 
2. [dl.cfg](dl.cfg): file contains the access and secret access key for AWS. 
3. [etl.py](etl.py): the main project file. It contains all the functions for the execution of the project as well as some docstrings explaining what each function do.
4. [Example.ipynb](Example.ipynb): File containing query examples for the processed files.

## Table Schema

After the original data is processed and transformed it is stored in parquet files following the schema below: 

- songs: the files are partitioned by year and artist. The songs table contains [song_id, title, duration]
- artist: no partitions. The artists table contains [artist_id, artist_name, artist_location, artist_latitude, artist_longitude] 
- users: no partitions. The users table contains [user_id, first_name, last_name, gender, level]
- time: the files are partitioned by year and month. The time table contains [start_time, hour, day, weekofyear, dayofweek]
- songplay: the files are partitioned by year and month. The sogplay table contains [start_time, user_id, level, song_id, artist_id, session_id, location, user_agent]



## Runtime

To run this function properly the user will need AWS access keys with read and write permissions to S3 and an EMR cluster. 
(WARNING: in the user try to run locally, it may take a long time to execute).  

1. Put the access key and secret access key in the respective fields in [dl.cfg](dl.cfg)
2. Change the INPUT and OUTPUT fields in the main function in [etl.py](etl.py) to match the paths of the buckets 

3. The next step will go according to the user choice of environment: 
    1. Local machine: open a python 3 console and type `!python3 etl.py`
    2. EMR Cluster: Copy both files (dl.cfg and etl.py) to an EMR Cluster and run etl.py (see this [link](https://knowledge.udacity.com/questions/46619#552992) for instructions how to create the EMR Cluster, copy the files and run them)

After execution, the user may choose to run [Example.ipynb](Example.ipynb) to test the results of the process
