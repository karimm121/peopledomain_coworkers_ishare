import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os

#changing the test file

# Add the directory containing utils.py to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'get-cowr-data-person-api-v0')))

# Mock google.auth.default() before importing the module that uses it
with patch('google.auth.default', return_value=(MagicMock(), 'fake_project_id')):
    from utils import write_db_table, get_managers, get_country_retail_managers, set_age_and_tenures, replace_nan_to_null

class TestUtils(unittest.TestCase):

    @patch('utils.bigquery.Client')
    def test_write_db_table_success(self, mock_bigquery_client):
        mock_client_instance = mock_bigquery_client.return_value
        mock_job = mock_client_instance.load_table_from_dataframe.return_value
        mock_job.result.return_value = True

        df = pd.DataFrame({'col1': ['value1']})
        result = write_db_table('dataset', 'table', df)
        self.assertTrue(result)

    def test_get_country_retail_managers(self):
        df = pd.DataFrame({
            'EmployeeId': ['1', '2'],
            'JobTitle': ['Country Retail Manager', 'Other']
        })
        status, df_refined = get_country_retail_managers(df)
        self.assertTrue(status)
        self.assertEqual(df_refined['isCountryRetailManager'].iloc[0], 'Yes')

    def test_replace_nan_to_null(self):
        df = pd.DataFrame({
            'col1': [None, 'NaN', 'nan', 'NaT', 'None', 'value']
        })
        df_cleaned = replace_nan_to_null(df)
        self.assertEqual(df_cleaned['col1'].tolist(), ['', '', '', '', '', 'value'])

if __name__ == '__main__':
    unittest.main()