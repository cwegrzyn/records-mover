from typing import Dict
from typing_inspect import get_args
from typing_extensions import TypedDict
import unittest

from records_mover.db.redshift.records_copy import redshift_copy_options
from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.records.delimited import complain_on_unhandled_hints
from records_mover.records.delimited.types import (
    HintDateFormat, HintDateTimeFormatTz, HintDateTimeFormat, PartialRecordsHints
)

# TODO: Move elsewhere

class TestRedshiftCopyOptions(unittest.TestCase):
    def test_delimited_time_format(self) -> None:
        # TODO: Ensure all items in HintDateFormat are handled
        class TimeFormatTestCase(TypedDict):
            datetimeformattz: HintDateTimeFormatTz
            datetimeformat: HintDateTimeFormat
            expected_time_format: str

        tests_by_date_format: Dict[HintDateFormat, TimeFormatTestCase] = {
            'DD-MM-YY': {
                'datetimeformattz': 'DD-MM-YY HH24:MIOF',
                'datetimeformat': 'DD-MM-YY HH24:MI',
                'expected_time_format': 'auto'
            },
            'YYYY-MM-DD': {
                'datetimeformattz': 'YYYY-MM-DD HH24:MI:SSOF',
                'datetimeformat': 'YYYY-MM-DD HH24:MI:SS',
                'expected_time_format': 'auto'
            },
            'MM-DD-YY': {
                'datetimeformattz': 'MM-DD-YY HH24:MIOF',
                'datetimeformat': 'MM-DD-YY HH24:MI',
                'expected_time_format': 'auto'
            },
            'DD-MM-YYYY': {
                'datetimeformattz': 'DD-MM-YYYY HH24:MIOF',
                'datetimeformat': 'DD-MM-YYYY HH24:MI',
                'expected_time_format': 'auto'
            },
            'MM-DD-YYYY': {
                'datetimeformattz': 'MM-DD-YYYY HH24:MIOF',
                'datetimeformat': 'MM-DD-YYYY HH24:MI',
                'expected_time_format': 'auto'
            },
            'DD/MM/YY': {
                'datetimeformattz': 'DD/MM/YY HH24:MIOF',
                'datetimeformat': 'DD/MM/YY HH24:MI',
                'expected_time_format': 'auto'
            },
            'MM/DD/YY': {
                'datetimeformattz': 'MM/DD/YY HH24:MIOF',
                'datetimeformat': 'MM/DD/YY HH24:MI',
                'expected_time_format': 'auto'
            },
            'DD/MM/YYYY': {
                'datetimeformattz': 'DD/MM/YYYY HH24:MIOF',
                'datetimeformat': 'DD/MM/YYYY HH24:MI',
                'expected_time_format': 'auto'
            },
            'MM/DD/YYYY': {
                'datetimeformattz': 'MM/DD/YYYY HH24:MIOF',
                'datetimeformat': 'MM/DD/YYYY HH24:MI',
                'expected_time_format': 'auto'
            },
        }
        for date_format in list(get_args(HintDateFormat)):
            test = tests_by_date_format[date_format]
            hints: PartialRecordsHints = {
                'dateformat': date_format,
                'datetimeformat': test['datetimeformat'],
                'datetimeformattz': test['datetimeformattz'],
            }
            records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                    hints=hints)
            unhandled_hints = set(records_format.hints.keys())
            redshift_options = redshift_copy_options(unhandled_hints,
                                                     records_format,
                                                     fail_if_cant_handle_hint=True,
                                                     fail_if_row_invalid=True,
                                                     max_failure_rows=0)
            complain_on_unhandled_hints(fail_if_dont_understand=True,
                                        unhandled_hints=unhandled_hints,
                                        hints=records_format.hints)
            self.assertEqual(redshift_options['time_format'], test['expected_time_format'])
