import unittest

from google.cloud.exceptions import NotFound
from records_mover.db.bigquery.loader import BigQueryLoader
from records_mover.records.records_format import (
    DelimitedRecordsFormat, ParquetRecordsFormat, AvroRecordsFormat
)
from records_mover.db.errors import NoTemporaryBucketConfiguration
from mock import MagicMock, Mock
from unittest.mock import patch


class TestBigQueryLoader(unittest.TestCase):
    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load_with_bad_schema_name(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                          gcs_temp_base_loc=None)
        mock_schema = 'my_project.my_dataset.something_invalid'
        mock_table = Mock(name='mock_table')
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format =\
            Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_directory.scheme = 'gs'
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        with self.assertRaises(ValueError):
            big_query_loader.load(schema=mock_schema, table=mock_table,
                                  load_plan=mock_load_plan,
                                  directory=mock_directory)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load_with_default_project(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                          gcs_temp_base_loc=None)
        mock_schema = 'my_dataset'
        mock_table = 'my_table'
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.format_type = 'delimited'
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_directory.scheme = 'gs'
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        mock_connection = mock_db.engine.raw_connection.return_value.connection
        mock_client = mock_connection._client
        mock_client.get_table.return_value.location = 'some-location'
        mock_job = mock_client.load_table_from_uri.return_value
        mock_job.output_rows = 42
        out = big_query_loader.load(schema=mock_schema, table=mock_table,
                                    load_plan=mock_load_plan,
                                    directory=mock_directory)
        mock_client.get_table.assert_called_with('my_dataset.my_table')
        mock_client.load_table_from_uri.\
            assert_called_with([mock_url],
                               'my_dataset.my_table',
                               location='some-location',
                               job_config=mock_load_job_config.return_value)
        mock_job.result.assert_called_with()

        self.assertEqual(out, mock_job.output_rows)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = None
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                          gcs_temp_base_loc=mock_gcs_temp_base_loc)
        mock_schema = 'my_project.my_dataset'
        mock_table = 'mytable'
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.format_type = 'delimited'
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_directory.scheme = 'gs'
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        mock_connection = mock_db.engine.raw_connection.return_value.connection
        mock_client = mock_connection._client
        mock_job = mock_client.load_table_from_uri.return_value
        mock_job.output_rows = 42
        mock_client.get_table.return_value.location = 'some-location'
        out = big_query_loader.load(schema=mock_schema, table=mock_table,
                                    load_plan=mock_load_plan,
                                    directory=mock_directory)
        mock_client.load_table_from_uri.\
            assert_called_with([mock_url],
                               'my_project.my_dataset.mytable',
                               location='some-location',
                               job_config=mock_load_job_config.return_value)
        mock_job.result.assert_called_with()

        self.assertEqual(out, mock_job.output_rows)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load_with_job_failure(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = None
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                          gcs_temp_base_loc=mock_gcs_temp_base_loc)
        mock_schema = 'my_project.my_dataset'
        mock_table = 'mytable'
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.format_type = 'delimited'
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_directory.scheme = 'gs'
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        mock_connection = mock_db.engine.raw_connection.return_value.connection
        mock_client = mock_connection._client
        mock_job = mock_client.load_table_from_uri.return_value
        mock_job.result.side_effect = Exception('some errors')

        mock_client.get_table.return_value.location = 'some-location'

        with self.assertRaises(Exception):
            big_query_loader.load(schema=mock_schema, table=mock_table,
                                  load_plan=mock_load_plan,
                                  directory=mock_directory)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load_no_table(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = None
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                          gcs_temp_base_loc=mock_gcs_temp_base_loc)
        mock_schema = 'my_project.my_dataset'
        mock_table = 'mytable'
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.format_type = 'delimited'
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_directory.scheme = 'gs'
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        mock_connection = mock_db.engine.raw_connection.return_value.connection
        mock_client = mock_connection._client
        mock_client.get_table.side_effect = NotFound('missing table')
        with self.assertRaises(NotFound):
            big_query_loader.load(schema=mock_schema, table=mock_table,
                                  load_plan=mock_load_plan,
                                  directory=mock_directory)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load_from_fileobj_true(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = None
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                          gcs_temp_base_loc=mock_gcs_temp_base_loc)
        mock_schema = 'my_project.my_dataset'
        mock_table = 'mytable'
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.format_type = 'delimited'
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_directory.scheme = 'gs'
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        mock_connection = mock_db.engine.raw_connection.return_value.connection
        mock_client = mock_connection._client
        mock_job = mock_client.load_table_from_file.return_value
        mock_job.output_rows = 42
        mock_fileobj = MagicMock(name='fileobj')
        out = big_query_loader.load_from_fileobj(schema=mock_schema,
                                                 table=mock_table,
                                                 load_plan=mock_load_plan,
                                                 fileobj=mock_fileobj)
        mock_client.load_table_from_file.\
            assert_called_with(mock_fileobj,
                               'my_project.my_dataset.mytable',
                               job_config=mock_load_job_config.return_value)
        mock_job.result.assert_called_with()

        self.assertEqual(out, mock_job.output_rows)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load_from_fileobj_error(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = None
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                          gcs_temp_base_loc=mock_gcs_temp_base_loc)
        mock_schema = 'my_project.my_dataset'
        mock_table = 'mytable'
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.format_type = 'delimited'
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_directory.scheme = 'gs'
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        mock_connection = mock_db.engine.raw_connection.return_value.connection
        mock_client = mock_connection._client
        mock_job = mock_client.load_table_from_file.return_value
        mock_job.output_rows = 42
        mock_fileobj = MagicMock(name='fileobj')
        mock_job.result.side_effect = Exception
        with self.assertRaises(Exception):
            big_query_loader.load_from_fileobj(schema=mock_schema,
                                               table=mock_table,
                                               load_plan=mock_load_plan,
                                               fileobj=mock_fileobj)
        mock_client.load_table_from_file.\
            assert_called_with(mock_fileobj,
                               'my_project.my_dataset.mytable',
                               job_config=mock_load_job_config.return_value)
        mock_job.result.assert_called_with()

    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load_with_fileobj_fallback(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = None
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                          gcs_temp_base_loc=mock_gcs_temp_base_loc)
        mock_schema = 'my_project.my_dataset'
        mock_table = 'mytable'
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.format_type = 'delimited'
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_directory.scheme = 'gs'
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        mock_connection = mock_db.engine.raw_connection.return_value.connection
        mock_client = mock_connection._client
        mock_job = mock_client.load_table_from_file.return_value
        mock_job.output_rows = 42

        mock_directory = Mock(name='directory')
        mock_directory.scheme = 's3'
        mock_file_url = MagicMock(name='file_url')
        mock_directory.manifest_entry_urls.return_value = [mock_file_url]
        mock_file_loc = mock_url_resolver.file_url.return_value
        mock_fileobj = mock_file_loc.open.return_value.__enter__.return_value

        out = big_query_loader.load(schema=mock_schema,
                                    table=mock_table,
                                    load_plan=mock_load_plan,
                                    directory=mock_directory)
        mock_client.load_table_from_file.\
            assert_called_with(mock_fileobj,
                               'my_project.my_dataset.mytable',
                               job_config=mock_load_job_config.return_value)
        mock_job.result.assert_called_with()

        self.assertEqual(out, mock_job.output_rows)

    def test_known_supported_records_formats_for_load(self):
        mock_db = Mock(name='db')
        mock_url_resolver = Mock(name='url_resolver')
        bigquery_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=None)
        out = bigquery_loader.known_supported_records_formats_for_load()
        self.assertEqual(3, len(out))
        delimited_records_format = out[0]
        self.assertEqual(type(delimited_records_format), DelimitedRecordsFormat)
        self.assertEqual('bigquery', delimited_records_format.variant)
        parquet_records_format = out[1]
        self.assertEqual(type(parquet_records_format), ParquetRecordsFormat)
        avro_records_format = out[2]
        self.assertEqual(type(avro_records_format), AvroRecordsFormat)

    def test_temporary_gcs_directory_loc_none(self):
        mock_db = Mock(name='db')
        mock_url_resolver = Mock(name='url_resolver')
        bigquery_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=None)
        with self.assertRaises(NoTemporaryBucketConfiguration):
            with bigquery_loader.temporary_gcs_directory_loc():
                pass

    def test_temporary_loadable_directory_loc(self):
        mock_db = Mock(name='db')
        mock_url_resolver = Mock(name='url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        bigquery_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=mock_gcs_temp_base_loc)
        with bigquery_loader.temporary_loadable_directory_loc() as loc:
            self.assertEqual(loc,
                             mock_gcs_temp_base_loc.temporary_directory.return_value.__enter__.
                             return_value)

    def test_temporary_gcs_directory_loc(self):
        mock_db = Mock(name='db')
        mock_url_resolver = Mock(name='url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        bigquery_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=mock_gcs_temp_base_loc)
        with bigquery_loader.temporary_gcs_directory_loc() as loc:
            self.assertEqual(loc,
                             mock_gcs_temp_base_loc.temporary_directory.return_value.__enter__.
                             return_value)

    def test_has_temporary_loadable_directory_loc_true(self):
        mock_db = Mock(name='db')
        mock_url_resolver = Mock(name='url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        bigquery_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=mock_gcs_temp_base_loc)
        self.assertTrue(bigquery_loader.has_temporary_loadable_directory_loc())

    def test_temporary_loadable_directory_scheme(self):
        mock_db = Mock(name='db')
        mock_url_resolver = Mock(name='url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        bigquery_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=mock_gcs_temp_base_loc)
        self.assertEqual('gs', bigquery_loader.temporary_loadable_directory_scheme())

    def test_best_scheme_to_load_from(self):
        mock_db = Mock(name='db')
        mock_url_resolver = Mock(name='url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        bigquery_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=mock_gcs_temp_base_loc)
        self.assertEqual('gs', bigquery_loader.best_scheme_to_load_from())
