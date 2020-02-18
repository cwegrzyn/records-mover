from records_mover.db.vertica.loader import VerticaLoader
from records_mover.records.records_format import DelimitedRecordsFormat
import unittest
from mock import patch, Mock


class TestVerticaLoader(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.db.vertica.loader.RecordsLoadPlan')
    @patch('records_mover.db.vertica.loader.ProcessingInstructions')
    @patch('records_mover.db.vertica.loader.vertica_import_options')
    def test_can_unload_this_format_with_s3_true(self,
                                                 mock_vertica_import_options,
                                                 mock_ProcessingInstructions,
                                                 mock_RecordsLoadPlan):
        mock_url_resolver = Mock(name='url_resolver')
        mock_db = Mock(name='db')

        mock_source_records_format = Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        mock_processing_instructions = mock_ProcessingInstructions.return_value
        mock_load_plan = mock_RecordsLoadPlan.return_value
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_load_plan.records_format.hints = {}

        vertica_loader = VerticaLoader(url_resolver=mock_url_resolver, db=mock_db)
        mock_source_records_format.hints = {}
        out = vertica_loader.can_load_this_format(mock_source_records_format)
        mock_vertica_import_options.assert_called_with(set(), mock_load_plan)
        mock_RecordsLoadPlan.\
            assert_called_with(processing_instructions=mock_processing_instructions,
                               records_format=mock_source_records_format)
        self.assertEqual(True, out)
