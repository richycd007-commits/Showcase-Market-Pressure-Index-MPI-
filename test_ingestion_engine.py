import unittest
from unittest.mock import MagicMock, patch
import sys
import io
from contextlib import redirect_stdout

# Mock dependencies that are missing in the environment
sys.modules["pandas"] = MagicMock()
sys.modules["sqlalchemy"] = MagicMock()

import ingestion_engine

class TestIngestionEngine(unittest.TestCase):

    @patch('ingestion_engine.create_engine')
    @patch('ingestion_engine.parse_args')
    @patch('ingestion_engine.ingest_and_clean_data')
    def test_main_database_connection_failure(self, mock_ingest, mock_parse_args, mock_create_engine):
        # Setup mocks
        mock_args = MagicMock()
        mock_args.db_url = "invalid_url"
        mock_args.csv_dir = "some_dir"
        mock_parse_args.return_value = mock_args

        mock_create_engine.side_effect = Exception("Connection failed")

        # Capture stdout
        f = io.StringIO()
        with redirect_stdout(f):
            ingestion_engine.main()

        output = f.getvalue()

        # Assertions
        self.assertIn("[-] Failed to connect to database: Connection failed", output)
        mock_ingest.assert_not_called()

if __name__ == '__main__':
    unittest.main()
