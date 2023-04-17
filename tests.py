
import pandas as pd
from main import add_filename_from_path

def test_add_filename_from_path(spark):
    expected_out = pd.DataFrame({
        'filename': ['another note.md', 'one note.md']
    })

    out = (
        spark.read.text('fixtures/', wholetext=True)
        .transform(add_filename_from_path)
        .select('filename')
        .toPandas())

    pd.testing.assert_frame_equal(out, expected_out)
    