import unittest
from unittest.mock import patch, Mock
import records_mover


@patch('google.auth.default')
@patch('google.cloud.storage.Client')
@patch('records_mover.session.get_config')
@patch('records_mover.session.set_stream_logging')
class TestTopLevel(unittest.TestCase):
    def test_implicit_session_sets_stream_logging(self,
                                                  mock_set_stream_logging,
                                                  mock_get_config,
                                                  mock_Client,
                                                  mock_google_auth_default):
        _ = records_mover.sources
        mock_set_stream_logging.assert_called()

    def test_sources(self,
                     mock_set_stream_logging,
                     mock_get_config,
                     mock_Client,
                     mock_google_auth_default):
        mock_credentials = Mock(name='credentials')
        mock_project = Mock(name='project')
        mock_google_auth_default.return_value = (mock_credentials, mock_project)
        self.assertEqual(type(records_mover.sources),
                         records_mover.records.sources.factory.RecordsSources)

    def test_targets(self,
                     mock_set_stream_logging,
                     mock_get_config,
                     mock_Client,
                     mock_google_auth_default):
        mock_credentials = Mock(name='credentials')
        mock_project = Mock(name='project')
        mock_google_auth_default.return_value = (mock_credentials, mock_project)
        self.assertEqual(type(records_mover.targets),
                         records_mover.records.targets.factory.RecordsTargets)
