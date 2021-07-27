"""
Tests the handling of dataframe conversion via pandas in the fileobjs source.
"""

from io import BytesIO
from records_mover.records.sources.uninferred_fileobjs import UninferredFileobjsRecordsSource
from records_mover.records.processing_instructions import ProcessingInstructions
from nose.tools import assert_equal

import pandas as pd
from pandas.testing import assert_frame_equal


def test_to_dataframes_source():
    """Test that a moderately complex dataframe is loaded from fileobj correctly."""
    files = {
        "test.csv": BytesIO(b'col1,col2,col3\n1,,foo\n2,2,asd\n')
    }
    pi = ProcessingInstructions()
    with UninferredFileobjsRecordsSource(target_names_to_input_fileobjs=files) \
            .to_fileobjs_source(pi) as fileobjsource:
        with fileobjsource.to_dataframes_source(pi) as source:
            assert_equal(source.records_schema.fields[1].field_type, 'integer')
            assert_frame_equal(
                pd.concat(source.dfs),
                pd.DataFrame({'col1': pd.Series([1, 2], dtype='Int64'),
                              'col2': pd.Series([None, 2], dtype='Int64'),
                              'col3': pd.Series(['foo', 'asd'], dtype='object')}))


def test_to_dataframes_source__nullable_int():
    """Test that nullable integers are loaded correctly."""
    files = {
        "test.csv": BytesIO(b'cola,colb\n1,\n,2\n')
    }
    pi = ProcessingInstructions()
    # Note that the python CSV sniffer does not appear to detect that this has a header row
    with UninferredFileobjsRecordsSource(target_names_to_input_fileobjs=files,
                                         initial_hints={'header-row': True}) \
            .to_fileobjs_source(pi) as fileobjsource:
        with fileobjsource.to_dataframes_source(pi) as source:
            assert_equal(source.records_schema.fields[0].field_type, 'integer')
            assert_equal(source.records_schema.fields[1].field_type, 'integer')
            assert_frame_equal(
                pd.concat(source.dfs),
                pd.DataFrame({'cola': pd.Series([1, None], dtype='Int64'),
                              'colb': pd.Series([None, 2], dtype='Int64')}))
