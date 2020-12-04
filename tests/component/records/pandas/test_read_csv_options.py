import unittest
from typing import Dict
from typing_extensions import TypedDict
from typing_inspect import get_args
from records_mover.records.delimited.types import (
    HintDateFormat, HintDateTimeFormat, HintDateTimeFormatTz
)
from records_mover.records.pandas.read_csv_options import pandas_read_csv_options
from records_mover.records.schema import RecordsSchema
from records_mover.records import DelimitedRecordsFormat, ProcessingInstructions


class TestReadCsvOptions(unittest.TestCase):
    def test_bzip(self):
        records_format = DelimitedRecordsFormat(hints={
            'compression': 'BZIP'
        })
        records_schema = RecordsSchema.from_data({
            'schema': 'bltypes/v1'
        })
        unhandled_hints = set(records_format.hints)
        processing_instructions = ProcessingInstructions()
        expectations = {
            'compression': 'bz2'
        }
        out = pandas_read_csv_options(records_format,
                                      records_schema,
                                      unhandled_hints,
                                      processing_instructions)
        self.assertTrue(all(item in out.items() for item in expectations.items()))

    def test_dateformat(self) -> None:
        class DateFormatExpectations(TypedDict):
            dayfirst: bool

        class DateFormatTestCase(TypedDict):
            # Use the datetimeformat/datetimeformattz which is
            # compatible, as pandas doesn't let you configure those
            # separately
            datetimeformat: HintDateTimeFormat
            datetimeformattz: HintDateTimeFormatTz
            expectations: DateFormatExpectations

        testcases: Dict[HintDateFormat, DateFormatTestCase] = {
            'YYYY-MM-DD': {
                'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
                'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                'expectations': {
                    'dayfirst': False,
                }
            },
            'MM-DD-YYYY': {
                'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
                'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                'expectations': {
                    'dayfirst': False,
                }
            },
            'DD-MM-YYYY': {
                'datetimeformat': 'DD-MM-YYYY HH:MI:SS',
                'datetimeformattz': 'DD-MM-YYYY HH:MI:SSOF',
                'expectations': {
                    'dayfirst': True,
                }
            },
        }
        for dateformat in list(get_args(HintDateFormat)):
            test = testcases[dateformat]
            records_format = DelimitedRecordsFormat(hints={
                'dateformat': dateformat,
                'datetimeformat': test['datetimeformat'],
                'datetimeformattz': test['datetimeformattz'],
            })
            records_schema = RecordsSchema.from_data({
                'schema': 'bltypes/v1',
                'fields': {},
            })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions()
            expectations = testcases[dateformat]['expectations']
            try:
                out = pandas_read_csv_options(records_format,
                                              records_schema,
                                              unhandled_hints,
                                              processing_instructions)
            except NotImplementedError:
                self.fail(f'Could not handle combination for {dateformat}')
            self.assertTrue(all(item in out.items() for item in expectations.items()))
