from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd

DB_PATH = '/home/jovyan/work/spark-warehouse/'
AVG_WORDS_PER_MINUTE_ADULTS = 238  #https://www.sciencedirect.com/science/article/abs/pii/S0749596X19300786
types = ['✍️OwnPosts', '📝CuratedNotes', '✒️SummarizedBooks', '🗞️Articles', '📚Books', 
         '🎙️Podcasts', '📜Papers', '🗣️Talks', '🦜FavoriteQuotes']

spark = (
    SparkSession.builder
    .master("local[*]")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.warehouse.dir", DB_PATH)
    .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:{DERBY_DB_DIR};create=true") 
    .enableHiveSupport()
    .getOrCreate()
)
# Set hive dynamic partition mode as non strict. Not entirely sure yet why I need this.
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

def create_hive_db():
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS obsidian_metrics_db LOCATION '/home/jovyan/work/spark-warehouse/';
        USE obsidian_metrics_db;
        CREATE TABLE IF NOT EXISTS files_metrics (
            date DATE,
            filename STRING,
            word_count INT,
            type STRING,
            tags ARRAY<string>
        )
        PARTITIONED BY (year INT, month INT)
        STORED AS PARQUET;
    """)


          
file_words_df = (
    spark.read.text("/home/jovyan/work/pages/", wholetext=True)
    .withColumn(
        "filename", 
        f.regexp_replace(
            f.reverse(
                f.split(
                    f.input_file_name(), 
                    '/')
                )[0],
            "%20", " "))
    .withColumn("word_count", f.size(f.split(f.col('value'), ' ')))
    .orderBy('word_count', ascending=False)
    .select(["filename", "word_count"])
)
file_words_df.toPandas()

