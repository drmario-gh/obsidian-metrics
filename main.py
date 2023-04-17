from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd

DB_PATH = '/home/jovyan/work/spark-warehouse/'
DERBY_DB_DIR = '/home/jovyan/work/derby/'
AVG_WORDS_PER_MINUTE_ADULTS = 238  #https://www.sciencedirect.com/science/article/abs/pii/S0749596X19300786
NOTE_TYPES = [
    '✍️OwnPosts', '📝CuratedNotes', '✒️SummarizedBooks', '🗞️Articles', '📚Books', 
    '🎙️Podcasts', '📜Papers', '🗣️Talks', '🦜FavoriteQuotes']

# spark = (
#     SparkSession.builder
#     .master("local[*]")
#     .config("spark.sql.catalogImplementation", "hive")
#     .config("spark.sql.warehouse.dir", DB_PATH)
#     .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:{DERBY_DB_DIR};create=true") 
#     .enableHiveSupport()
#     .getOrCreate())
# # Not entirely sure yet why I need this but related to 'saveAsTable' below.
# spark.sql("set hive.exec.dynamic.partition.mode=nonstrict") 
# spark.sql("USE obsidian_metrics_db")

def create_hive_db():
    """
    I will only run this manually at the beginning of the project.
    """
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS obsidian_metrics_db LOCATION '/home/jovyan/work/spark-warehouse/';
        USE obsidian_metrics_db;
        CREATE TABLE IF NOT EXISTS files_metrics (
            date DATE,
            filename STRING,i
            word_count INT,
            type STRING,
            tags ARRAY<string>
        )
        PARTITIONED BY (year INT, month INT)
        STORED AS PARQUET;
    """)

def add_filename_from_path(df):
    return (
        df
        .withColumn(
            "filename", 
            f.regexp_replace(
                f.reverse(
                    f.split(f.input_file_name(), '/')
                    )[0],
                "%20", " ")))

def clean_up_tag(tag):
    return (
         tag
        .pipe(f.regexp_replace, "\[\[|\]\]", "")
        .pipe(f.regexp_replace, "\#", "")
        .pipe(f.regexp_replace, "\.$", "")
        .pipe(f.trim))

def extract_tags_into_array(tags_col):
    return (
        tags_col
        .pipe(f.split, "::")[1]
        .pipe(f.split, "\s*,\s*\[\[|\s*,\s*\#"))

def add_tags(df):
    """
    df[filename, value], where each row is a line of the file.
    """
    return (
        df
        .filter(f.col("value").rlike("Tags::"))
        .withColumn("tags",
            f.col("value")
            .pipe(extract_tags_into_array)
            .pipe(f.transform, clean_up_tag)
            .pipe(f.array_remove, ""))
        .withColumn("type", 
                    f.element_at(
                        f.array_intersect(f.array(*[f.lit(t) for t in NOTE_TYPES]), f.col("tags")),
                        1
                    )
        )
        .withColumn("tags", f.array_except(f.col("tags"), f.array(f.col("type")))))

# # Merge the two dataframes
# file_metrics_df = file_words_df.join(tags_df, on="filename", how="left")
# file_metrics_df = file_metrics_df.withColumn("date", f.current_date())
# file_metrics_df.toPandas()

# file_metrics_df = (
#     file_metrics_df
#     .withColumn("year", f.year(f.col("date")))
#     .withColumn("month", f.month(f.col("date")))
#     .withColumn("date", f.col("date").cast("date"))
#     .withColumn("word_count", f.col("word_count").cast("int"))
#     .withColumn("tags", f.col("tags").cast("array<string>"))
# )

# # Append file_metrics_df to the corresponding partition of file_metrics hive table
# file_metrics_df.write.format("hive").mode("append").partitionBy("year", "month").saveAsTable("obsidian_metrics_db.files_metrics")

# # total_reading_hours rounded 2 decimals
# total_reading_hours = round(file_metrics_df.agg(f.sum("word_count")).collect()[0][0] / AVG_WORDS_PER_MINUTE_ADULTS / 60, 2)

# # Get total_pages
# total_pages = file_metrics_df.count()

# # reading hours and pages by type
# by_type = (
#     file_metrics_df
#     .groupBy("type")
#     .agg(
#         f.sum("word_count").alias("total_words"),
#         f.count("filename").alias("total_pages")
#     )
#     .withColumn("reading_hours", f.round(f.col("total_words") / AVG_WORDS_PER_MINUTE_ADULTS / 60, 2))
#     # Replace None with Other
#     .withColumn("type", f.when(f.col("type").isNull(), "Other").otherwise(f.col("type")))
#     .select(["type", "reading_hours", "total_pages"])
#     .orderBy("reading_hours", ascending=False)
# )

# by_type.toPandas()

# # Plot reading_hours by type with Plotly, showing the hours on top of each bar, rounded to 2 decimals
# # with each bar in a different color
# import plotly.express as px
# import plotly.graph_objects as go

# fig = px.bar(by_type.toPandas(), x="type", y="reading_hours", color="type", text="reading_hours")
# fig.update_layout(title_text=f"Total reading hours: {total_reading_hours}")
# fig.update_traces(texttemplate='%{text}', textposition='outside')
# fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
# fig.show()


# # Export the plot to a SVG file with the proper font so emojis show up
# fig.write_image("/home/jovyan/work/assets/reading_hours_by_type.svg")

# # Plot pages by type with Plotly, showing the hours on top of each bar, rounded to 2 decimals
# # with each bar in a different color
# import plotly.express as px
# import plotly.graph_objects as go

# fig = px.bar(by_type.toPandas(), x="type", y="total_pages", color="type", text="total_pages")
# fig.update_traces(texttemplate='%{text}', textposition='outside')
# fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
# fig.update_layout(title_text=f"Total pages: {total_pages}")
# fig.show()

# fig.write_image("/home/jovyan/work/assets/pages_by_type.svg")

# df = (
#     spark.read.text("/home/jovyan/work/pages/")
#     .withColumn(
#         "filename", 
#         f.regexp_replace(
#             f.reverse(
#                 f.split(
#                     f.input_file_name(), 
#                     '/')
#                 )[0],
#             "%20", " "))
#     # Filter by lines that have "Tags::"
#     .filter(f.col("value").rlike("Tags::"))
#     # Split by "::" and get the second element
#     .withColumn("tags", f.split(f.col("value"), "::")[1])
#     # Split by ", [["
#     .withColumn("tags", f.split(f.col("tags"), "\s*,\s*\[\[|\s*,\s*\#"))    
#     # Flatten the list of lists
#     .withColumn("tags", f.explode(f.col("tags")))
#     # Remove "[[" and "]]"
#     .withColumn("tags", f.regexp_replace(f.col("tags"), "\[\[|\]\]", ""))
#     # Remove the hashtags
#     .withColumn("tags", f.regexp_replace(f.col("tags"), "\#", ""))
#     # Remove final dot if any (e.g. "Python.")
#     .withColumn("tags", f.regexp_replace(f.col("tags"), "\.$", ""))
#     # Remove the spaces
#     .withColumn("tags", f.trim(f.col("tags")))
#     # Remove the empty strings
#     .filter(f.col("tags") != "")
#     # Remove tags that are in typeS
#     .filter(~f.col("tags").isin(typeS))
#     # Join by filename with the file_words_df
#     .join(file_words_df, on="filename", how="left")
#     # Group by tags case insensitive and count
#     .groupBy(f.col("tags"))
#     # Get the count of each tag and the number of words
#     .agg(
#         f.count("filename").alias("count"),
#         f.sum("word_count").alias("total_words")
#     )
#     # Get reading hours
#     .withColumn("reading_hours", f.round(f.col("total_words") / AVG_WORDS_PER_MINUTE_ADULTS / 60, 2))
#     # Remove from tags those in typeS
# )
# df = df.toPandas()
# df

# # Plot reading_hours by tag with Plotly, showing the hours on top of each bar, rounded to 2 decimals
# # with each bar in a different color
# import plotly.express as px
# import plotly.graph_objects as go

# # Get the top 10 tags by reading time
# top_10 = df.sort_values("reading_hours", ascending=False).head(10)
# # Truncate page numbers to 20 characters with ellipsis
# top_10["tags"] = top_10["tags"].apply(lambda x: x[:20] + "..." if len(x) > 20 else x)
# # Plot with a different palette
# fig = px.bar(
#     top_10, 
#     x="tags", 
#     y="reading_hours", 
#     color="tags", 
#     text="reading_hours",
#     color_discrete_sequence=px.colors.qualitative.Set3,
# )

# fig.update_traces(texttemplate='%{text}', textposition='outside')
# fig.update_layout(showlegend=False)
# fig.update_layout(title_text=f"Top 10 topics by reading hours")
# fig.update_xaxes(tickfont=dict(size=10))
# fig.show()

# fig.write_image("/home/jovyan/work/assets/topics_by_hour.svg")

# # Get the map of each page to the color used
# color_map = dict(zip(top_10["tags"], px.colors.qualitative.Set3))

# # Plot reading_hours by tag with Plotly, showing the hours on top of each bar, rounded to 2 decimals
# # with each bar in a different color
# import plotly.express as px
# import plotly.graph_objects as go

# # Get the top 10 tags by reading time
# top_10 = df.sort_values("count", ascending=False).head(10)
# # Truncate page numbers to 50 characters with ellipsis but put ellipsis only if the string is longer than 50 characters
# top_10["tags"] = top_10["tags"].apply(lambda x: x[:20] + "..." if len(x) > 20 else x)
# fig = px.bar(top_10, x="tags", y="count", color="tags", text="count")
# fig.update_traces(texttemplate='%{text}', textposition='outside')
# fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
# fig.update_layout(showlegend=False)
# fig.update_layout(title_text=f"Top 10 topics by number of pages")
# fig.update_xaxes(tickfont=dict(size=10))
# # For each page that is also on the previous graph, use that same color. If it wasn´t there, use the default color
# fig.for_each_trace(lambda t: t.update(marker_color=color_map.get(t.name, t.marker.color)))

# fig.show()

# fig.write_image("/home/jovyan/work/assets/topics_by_page.svg")

# (
#     spark.read.table("files_metrics")
#     .groupBy(f.col("date"))
#     .agg(
#         f.sum("word_count").alias("total_words")
#     )
#     .withColumn("previous_day", f.lag(f.col("total_words")).over(Window.orderBy(f.col("date"))))
#     .withColumn("diff", f.col("total_words") - f.col("previous_day"))
#     .toPandas()
# )

# # We have the files_metrics Hive table like
# # filename	word_count	tags	type	date
# # 0	📖 Extreme Programming Explained.md	5104	[📚Books, My Engineering Manager principles, va...	✍️OwnPosts	2023-04-03
# # 1	📖 Four Thousand Weeks.md	5027	[📚Books, Productivity, ⛵ Life vision, mission,...	✒️SummarizedBooks	2023-04-0
# # Explode the tags column and get the total number of words per tag per day
# df = (
#     spark.read.table("files_metrics")
#     .filter(f.col("date") >= (f.current_date() - f.expr("interval 7 days")))
#     .withColumn("tags", f.explode(f.col("tags")))
#     .groupBy(f.col("date"), f.col("tags"))
#     .agg(
#         f.sum("word_count").alias("total_words")
#     )
#     .toPandas()
# )
# # Set date as Date index
# df["date"] = pd.to_datetime(df["date"])
# df["previous_day"] = df.groupby("tags")["total_words"].shift(1)
# df["diff"] = df["total_words"] - df["previous_day"]

# # Sum diff by tags
# top_3 = df.groupby("tags")['diff'].sum().sort_values(ascending=False).head(3)

# # Plot top_5 with Plotly, showing the values on top of each bar with each bar in a different color
# # but using the colors in color_map if there is a value there for that tag
# import plotly.express as px
# import plotly.graph_objects as go

# # Truncate page numbers to 50 characters with ellipsis but put ellipsis only if the string is longer than 50 characters
# top_3 = top_3.reset_index()
# top_3["tags"] = top_3["tags"].apply(lambda x: x[:20] + "..." if len(x) > 20 else x)
# fig = px.bar(top_3, x="tags", y="diff", color="tags", text="diff")
# fig.update_traces(texttemplate='%{text}', textposition='outside')
# fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
# fig.update_layout(showlegend=False)
# fig.update_layout(title_text=f"Top 3 topics by words added in the last 7 days")
# fig.update_xaxes(tickfont=dict(size=10))
# # For each page that is also on the previous graph, use that same color. If it wasn´t there, use the default color
# fig.for_each_trace(lambda t: t.update(marker_color=color_map.get(t.name, t.marker.color)))

# fig.show()

# fig.write_image("/home/jovyan/work/assets/words_by_topic_last_7_days.svg")

# def main():
#     pass

# # Write Python main
# if __name__ == "__main__":
#     word_metrics_by_file = (
#         spark.read.text("/home/jovyan/work/pages/", wholetext=True)
#         .transform(add_filename_from_path)
#         .withColumn("word_count", f.size(f.split(f.col('value'), ' ')))
    
#     tags_by_file = (
#         spark.read.text("/home/jovyan/work/pages/")
#     )

#     main()
