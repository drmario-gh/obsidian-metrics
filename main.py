from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import plotly.express as px
import datetime
import pandas as pd
import hashlib

DEVCONTAINER_WORKSPACE_PATH = '/home/jovyan/work'
DB_PATH = f'{DEVCONTAINER_WORKSPACE_PATH}/spark-warehouse'
DERBY_DB_PATH = f'{DEVCONTAINER_WORKSPACE_PATH}/derby'
PLOTS_PATH = f'{DEVCONTAINER_WORKSPACE_PATH}/assets'
NOTES_PATH = f'{DEVCONTAINER_WORKSPACE_PATH}/pages'
RAW_HIGHLIGHTS_PATH = f'{DEVCONTAINER_WORKSPACE_PATH}/raw highlights'

AVG_WORDS_PER_MINUTE_ADULTS = 238  #https://www.sciencedirect.com/science/article/abs/pii/S0749596X19300786
NOTE_TYPES = [
    '✍️OwnPosts', '📝CuratedNotes', '✒️SummarizedBooks', '🗞️Articles', '📚Books', 
    '🎙️Podcasts', '📜Papers', '🗣️Talks', '🦜FavoriteQuotes']


def get_palette_color_from_tag(tag):
    """
    Maps any string to a hex color using md5 to make it consistent across executions.
    """
    return '#' + hashlib.md5(tag.encode('utf-8')).hexdigest()[:6]


class StatsCalculator():
    def __init__(self, spark_obj, notes_path):
        self.spark = spark_obj
        self.notes_path = notes_path
        self.notes_folder_name = notes_path.split('/')[-1].replace(' ', '_')
        
    def create_hive_db_and_table_if_not_exists(self):
        """
        I will only run this manually at the beginning of the project.
        Weird, Spark SQL doesn't support multiple SQL statements at once?
        """
        self.spark.sql("""
            CREATE DATABASE IF NOT EXISTS obsidian_metrics_db LOCATION '{DB_PATH}';
        """)
        self.spark.sql("USE obsidian_metrics_db;")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.notes_folder_name}_metrics (
                date DATE,
                filename STRING,
                word_count INT,
                type STRING,
                tags ARRAY<string>
            )
            USING hive OPTIONS(fileFormat 'parquet')
            PARTITIONED BY (year INT, month INT)
        """)

    def run(self):
        self.create_hive_db_and_table_if_not_exists()
        metrics_df = self._get_note_metrics()
        self._save_to_hive(metrics_df)

        metrics_by_type_df = self._get_metrics_by_type(metrics_df)
        self._plot_metric_by_type('reading_hours', metrics_by_type_df)
        self._plot_metric_by_type('total_pages', metrics_by_type_df)

        metrics_by_tag_df = self._get_metrics_by_tag(metrics_df)
        self._plot_top_10_metric_by_tag('reading_hours', metrics_by_tag_df)
        self._plot_top_10_metric_by_tag('total_pages', metrics_by_tag_df)

        df_diff = self._get_top_5_word_count_diff_7_days()
        self._plot_top_5_word_count_diff_7_days(df_diff)
        
        df_hours_by_day = self._get_reading_hours_per_day()
        self._plot_reading_hours_per_day(df_hours_by_day)

    def _get_note_metrics(self):
        words_df = (
            self.spark.read.text(
                self.notes_path, wholetext=True, recursiveFileLookup=True)
            .transform(self._add_filename_from_path)
            .select(
                "filename",
                f.size(f.split("value", ' ')).alias("word_count")))
        tags_df = (
            self.spark.read.text(
                self.notes_path, wholetext=False, recursiveFileLookup=True)
            .filter(f.col("value").rlike(".* Tags::"))
            .transform(self._add_filename_from_path)
            .transform(self._extract_clean_tags_into_array)
            .transform(self._move_note_type_tag_to_own_column)
            .select("filename", "tags", "type"))
        return (
            words_df.join(tags_df, on="filename", how="left")
            .select("*", f.current_date().alias("date")))

    def _add_filename_from_path(self, df):
        """
        Is this the best that can be done to avoid nesting calls?
        Repeated "withColumn" can have an impact on performance, 
        since Spark DFs are immutable.
        """
        return (df
            .select("*", f.reverse(f.split(f.input_file_name(), '/'))[0].alias("filename"))
            .withColumn("filename", f.regexp_replace("filename", "%20", " ")))

    def _extract_clean_tags_into_array(self, df_taglines):
        """
        df[[filename, value]], with value containing "Tags::..."
        """
        def _clean_up_tag(tag_col):
            return f.trim(f.regexp_replace(tag_col, "\[\[|\]\]|\#|\.$", ""))
        
        return (
            df_taglines
            .select("*", f.split("value", "::")[1].alias("tags"))
            .withColumn("tags", f.split("tags", "\s*,\s*\[\[|\s*,\s*\#"))
            .withColumn("tags", f.transform("tags", _clean_up_tag).alias("tags")))
    
    def _move_note_type_tag_to_own_column(self, df_tagarray):
        """
        df[[filename, tags]], with tags being an array of strings.
        """
        note_types_array = f.array(*[f.lit(t) for t in NOTE_TYPES])
        return (
            df_tagarray
            .select('*', f.element_at(f.array_intersect(note_types_array, "tags"), 1).alias("type"))
            .withColumn("tags", f.array_except("tags", note_types_array)))

    def _save_to_hive(self, df):
        return (df
            .select(
                "date",
                "filename",
                "word_count",
                "type",
                "tags",
                f.year("date").alias("year"),
                f.month("date").alias("month"))
            .write.format("hive").mode("append").partitionBy("year", "month")
            .saveAsTable(f"obsidian_metrics_db.{self.notes_folder_name}_metrics"))

    def _get_metrics_by_type(self, file_metrics_df):
        return (
            file_metrics_df
            .groupBy("type")
            .agg(
                f.sum("word_count").alias("total_words"),
                f.count("filename").alias("total_pages"))
            .select(
                f.when(f.col("type").isNull(), "No type").otherwise(f.col("type")).alias("type"),
                f.round(f.col("total_words") / AVG_WORDS_PER_MINUTE_ADULTS / 60, 2).alias("reading_hours"),
                "total_pages")
            .orderBy("reading_hours", ascending=False))
    
    def _plot_metric_by_type(self, metric, df_by_type):
        df = df_by_type.toPandas()
        fig = px.bar(df, x="type", y=metric, text=metric)
        fig.update_layout(
            title_text=f"Total {metric}: {df[metric].sum():.2f}",
            uniformtext_minsize=8, 
            uniformtext_mode='hide',
            showlegend=False)
        fig.update_traces(
            texttemplate='%{text}', 
            textposition='outside')
        fig.write_image(f"{PLOTS_PATH}/{metric}_by_type_for_{self.notes_folder_name}.svg")

    def _get_metrics_by_tag(self, file_metrics_df):
        return (
            file_metrics_df
            .select(
                f.explode("tags").alias("tag"),
                "filename",
                "word_count")
            .groupBy(f.lower(f.col("tag")).alias("tag"))
            .agg(
                f.sum("word_count").alias("total_words"),
                f.count("filename").alias("total_pages"))
            .select(
                "tag",
                f.round(f.col("total_words") / AVG_WORDS_PER_MINUTE_ADULTS / 60, 2).alias("reading_hours"),
                "total_pages")
            .orderBy("reading_hours", ascending=False))
        
    def _plot_top_10_metric_by_tag(self, metric, df_by_tag):
        """
        We return a colormap to be used in the next calls to this function as the `previous_color_map`.
        This mechanism allows us to keep the same colors for the same tags in different plots.
        """
        top_10 = df_by_tag.toPandas().sort_values(metric, ascending=False).head(10)
        top_10.loc[top_10["tag"]== '', "tag"] = "Untagged"
        top_10["tag"] = top_10["tag"].apply(lambda x: x[:20] + "..." if len(x) > 20 else x)
        top_10["color"] = top_10["tag"].apply(lambda x: get_palette_color_from_tag(x))

        fig = px.bar(top_10, x="tag", y=metric, color="color", text=metric, 
                     color_discrete_sequence=top_10["color"].tolist(), category_orders={"tag": top_10["tag"]},)
        fig.update_traces(texttemplate='%{text}', textposition='outside')
        fig.update_layout(
            showlegend=False, title_text=f"Top 10 tags by {metric}",
        )
        fig.update_xaxes(tickfont=dict(size=10))
        fig.write_image(f"{PLOTS_PATH}/tags_by_{metric}_for_{self.notes_folder_name}.svg")
        return fig

    def _get_top_5_word_count_diff_7_days(self):
        df = self._get_word_count_diff_14_days()
        df["previous_day"] = df.groupby("tag")["total_words"].shift(1).fillna(0)
        df = df[df["date"] >= (datetime.datetime.now().date() - pd.Timedelta(days=7))]
        df["diff"] = df["total_words"] - df["previous_day"]
        return (
            df.groupby("tag")['diff'].sum().sort_values(ascending=False).head(5).reset_index())
    
    def _get_records_from_14_days(self):
        """
        This is useful to debug weird counts from a Jupyter notebook
        """
        return (
            self.spark.read.table(f'{self.notes_folder_name}_metrics')
            .filter(f.col("date") >= (f.current_date() - f.expr("interval 14 days")))
            .select(
                "date",
                "filename",
                "word_count",
                f.explode(f.col("tags")).alias("tag")))
    
    def _get_word_count_diff_14_days(self):
        """
        This is useful to debug weird counts from a Jupyter notebook.
        I need to groupby by lowercasing the tag because Readwise is inconsistent 
        with the case of my tags. The good thing is that this does not cause any problem
        in Obsidian since wikilinks are case insensitive.
        """
        return (
            self._get_records_from_14_days()
            .groupBy(f.col("date"), f.lower(f.col("tag")).alias("tag"))
            .agg(f.sum("word_count").alias("total_words"))
            .orderBy("tag", "date")
            .toPandas())
        
        
    def _plot_top_5_word_count_diff_7_days(self, df_diff_7_days):
        df_diff_7_days.loc[df_diff_7_days["tag"]== '', "tag"] = "Untagged"
        df_diff_7_days["tag"] = df_diff_7_days["tag"].apply(lambda x: x[:20] + "..." if len(x) > 20 else x)
        df_diff_7_days["color"] = df_diff_7_days["tag"].apply(lambda x: get_palette_color_from_tag(x))
        fig = px.bar(df_diff_7_days, x="tag", y="diff", text="diff", color="color", 
                     color_discrete_sequence=df_diff_7_days["color"], category_orders={"tag": df_diff_7_days["tag"]})
        fig.update_traces(
            texttemplate='%{text}',
            textposition='outside')
        fig.update_layout(
            uniformtext_minsize=8, uniformtext_mode='hide',
            showlegend=False,
            title_text=f"Top 5 tags by words added in the last 7 days",
            xaxis_tickangle=30)
        fig.update_xaxes(tickfont=dict(size=10))
        fig.write_image(f"{PLOTS_PATH}/words_by_tag_last_7_days_for_{self.notes_folder_name}.svg")
        return fig
    
    def _get_reading_hours_per_day(self):
        return (
            spark.read.table(f'{self.notes_folder_name}_metrics')
            .groupBy('date')
            .agg(f.sum('word_count').alias('total_words'))
            .select(
                'date', 
                f.round(f.col("total_words") / AVG_WORDS_PER_MINUTE_ADULTS / 60, 2).alias("reading_hours"))
            .orderBy('date'))
    
    def _plot_reading_hours_per_day(self, df):
        df = df.toPandas()
        fig = px.line(df, x='date', y='reading_hours', title=f'Total reading hours in {self.notes_folder_name} per day')
        fig.update_xaxes(title_text='')
        fig.update_yaxes(title_text='')
        fig.write_image(f"{PLOTS_PATH}/reading_hours_per_day_for_{self.notes_folder_name}.svg")


def get_spark_instance():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", DB_PATH)
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:{DERBY_DB_PATH};create=true") 
        .enableHiveSupport()
        .getOrCreate())
    # Not entirely sure yet why I need this but related to 'saveAsTable' below.
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict") 
    spark.sql("use obsidian_metrics_db")
    return spark

if __name__ == "__main__":
    spark = get_spark_instance()
    StatsCalculator(spark, NOTES_PATH).run()
    StatsCalculator(spark, RAW_HIGHLIGHTS_PATH).run()


   