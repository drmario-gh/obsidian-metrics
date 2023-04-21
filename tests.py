
import pandas as pd
from main import StatsCalculator
import pyspark.sql.functions as f

def test_add_filename_from_path(spark):
    expected_out = pd.DataFrame({
        'filename': ['another note.md', 'one note.md']
    })
    
    stats_calculator = StatsCalculator(
        spark_obj=spark,
        notes_path='fixtures/')
    df = spark.read.text(stats_calculator.notes_path, wholetext=True)
    out = (
        stats_calculator
        ._add_filename_from_path(df)
        .select('filename')
        .toPandas())
    assert set(out['filename']) == set(expected_out['filename'])

def test_extract_clean_tags_into_array(spark):
    expected_out = pd.DataFrame({
        'tags': [
            ['🗞️Articles', 'A topic', 'Another topic'],
            ['📚Books', '✒️SummarizedBooks', 'A topic', 'Another topic']]})
    
    stats_calculator = StatsCalculator(
        spark_obj=spark,
        notes_path='fixtures/')
    df = (
        spark.read.text(stats_calculator.notes_path, wholetext=False)
        .filter(f.col("value").rlike("Tags::")))

    out = (
        stats_calculator
        ._extract_clean_tags_into_array(df)
        .select('tags')
        .toPandas()
    )
    pd.testing.assert_frame_equal(
        out.sort_values(by='tags').reset_index(drop=True), 
        expected_out.sort_values(by='tags').reset_index(drop=True),
        check_like=True
    )

# def test_add_tags(spark):
#     expected_out = pd.DataFrame({
#         'filename': ['another note.md', 'one note.md'],
#         'tags': ['tag1, tag2', 'tag3, tag4']
#     })
    
#     stats_calculator = StatsCalculator(
#         spark_obj=spark,
#         notes_path='fixtures/')
#     df = spark.read.text(stats_calculator.notes_path, wholetext=False)
#     df = stats_calculator._add_filename_from_path(df)
#     out = (
#         stats_calculator
#         ._add_tags(df)
#         .select('filename', 'tags')
#         .toPandas())
#     pd.testing.assert_frame_equal(out, expected_out)