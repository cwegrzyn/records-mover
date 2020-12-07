import unittest
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.db.vertica.records_import_options import vertica_import_options
from records_mover.records import DelimitedRecordsFormat, ProcessingInstructions
from ...records.datetime_cases import (
    DATE_CASES, create_sample, SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY
)


class TestVerticaImportOptions(unittest.TestCase):
    maxDiff = None

    def test_vertica_import_options_max_failure_rows_specified(self):
        records_format = DelimitedRecordsFormat(variant='vertica')
        unhandled_hints = set(records_format.hints)
        processing_instructions = ProcessingInstructions(max_failure_rows=123)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        out = vertica_import_options(unhandled_hints,
                                     load_plan)
        expectations = {
            'trailing_nullcols': True,
            'rejectmax': 123,
            'enforcelength': None,
            'error_tolerance': None,
            'abort_on_error': None,
        }
        self.assertTrue(set(expectations.items()).issubset(set(out.items())))

    def test_vertica_import_options_dateformat(self):
        should_raise = {
            'YYYY-MM-DD': False,
            'MM-DD-YYYY': True,
            'DD-MM-YYYY': True,
            'MM/DD/YY': True,
            'DD/MM/YY': True,
            'DD-MM-YY': True,
        }
        for dateformat in DATE_CASES:
            records_format = DelimitedRecordsFormat(variant='vertica',
                                                    hints={
                                                        'dateformat': dateformat
                                                    })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions(max_failure_rows=123)
            load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                        records_format=records_format)
            try:
                vertica_import_options(unhandled_hints, load_plan)
                # Records Mover passes no particular option for dateformat on
                # import in Vertica; it always uses YYYY-MM-DD as a result.
            except NotImplementedError:
                if should_raise[dateformat]:
                    pass
                else:
                    self.fail()
