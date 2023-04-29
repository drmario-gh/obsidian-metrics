
import datetime
import pandas as pd
import pyspark.sql.functions as f
import pytest

from main import StatsCalculator


def test_add_filename_from_path(spark):
    expected_out = pd.DataFrame({
        'filename': ['another note.md', 'one note.md'],
    })
    stats_calculator = StatsCalculator(
        spark_obj=spark,
        notes_path='fixtures/')
    df = spark.read.text(stats_calculator.notes_path, wholetext=True)
    
    out = stats_calculator._add_filename_from_path(df)
    
    assert 'value' in out.columns
    assert set(out.toPandas()['filename']) == set(expected_out['filename'])

def test_extract_clean_tags_into_array(spark):
    expected_out = pd.DataFrame({
        'tags': [
            ['🗞️Articles', 'A topic', 'Another topic', 'And another topic'],
            ['📚Books', '✒️SummarizedBooks', 'A topic', 'Another topic']]})
    stats_calculator = StatsCalculator(
        spark_obj=spark,
        notes_path='fixtures/')
    df = (
        spark.read.text(stats_calculator.notes_path, wholetext=False)
        .filter(f.col("value").rlike("Tags::")))

    out = stats_calculator._extract_clean_tags_into_array(df)
    
    assert {'value', 'tags'}.issubset(set(out.columns))
    pd.testing.assert_frame_equal(
        out.toPandas()[['tags']].sort_values(by='tags').reset_index(drop=True), 
        expected_out.sort_values(by='tags').reset_index(drop=True),
        check_like=True)

def test_add_note_type_tag(spark):
    input_df = spark.createDataFrame(
        data=[
            ('one note', ['🗞️Articles', 'A topic', 'Another topic', 'And another topic']),
            ('another note', ['📚Books', '✒️SummarizedBooks', 'A topic', 'Another topic'])
        ],
        schema=['filename', 'tags'])
    expected_out = pd.DataFrame({
        'type': ['🗞️Articles', '✒️SummarizedBooks'],
        'tags': [
            ['A topic', 'Another topic', 'And another topic'],
            ['A topic', 'Another topic']]})
    stats_calculator = StatsCalculator(
        spark_obj=spark,
        notes_path='fixtures/')


    out = stats_calculator._move_note_type_tag_to_own_column(input_df)

    pd.testing.assert_frame_equal(
        out.toPandas()[["tags", "type"]].sort_values(by='tags').reset_index(drop=True), 
        expected_out.sort_values(by='tags').reset_index(drop=True),
        check_like=True)

@pytest.mark.usefixtures("f_current_date_mock")
def test_get_note_metrics(spark):
    expected_out = (
        pd.DataFrame({
            'filename': ['another note.md', 'one note.md'],
            'word_count': [13, 10],
            'tags': [
                ['A topic', 'Another topic', 'And another topic'],
                ['A topic', 'Another topic']],
            'type': ['🗞️Articles', '✒️SummarizedBooks']})
        .assign(
            word_count=lambda x: x['word_count'].astype('int32'),
            date = datetime.date(2020, 1, 1)))          
    stats_calculator = StatsCalculator(spark_obj=spark, notes_path='fixtures/')

    out = stats_calculator._get_note_metrics()

    pd.testing.assert_frame_equal(
        out.toPandas().sort_values(by='filename').reset_index(drop=True),
        expected_out.sort_values(by='filename').reset_index(drop=True),
        check_like=True)

