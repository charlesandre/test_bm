import unittest
import pandas as pd
from unittest import mock
from transformater.utils import download_file_from_s3_public_bucket, UNSIGNED, csv_file_to_parquet, parquet_to_df, split_dataframe

class TestUtils(unittest.TestCase):
    @mock.patch("transformater.utils.boto3")
    @mock.patch("transformater.utils.Config")
    def test_download_file_from_s3_public_bucket(self, mock_config, mock_boto3):
        download_file_from_s3_public_bucket("my_bucket", "my_object", "my_destination_name")
        mock_config.assert_called_once_with(signature_version=UNSIGNED)
        mock_boto3.client.assert_called_once_with('s3', config=mock_config.return_value)
        mock_boto3.client.return_value.download_file.assert_called_once_with("my_bucket", "my_object", "my_destination_name")

    @mock.patch("transformater.utils.pd")
    def test_csv_file_to_parquet(self, mock_pd):
        csv_file_to_parquet("my_input_file", "my_output_file")
        mock_pd.read_csv.assert_called_once_with("my_input_file")
        mock_pd.read_csv.return_value.to_parquet.assert_called_once_with("my_output_file")

    @mock.patch("transformater.utils.pd")
    def test_parquet_to_df(self, mock_pd):
        parquet_to_df("my_input_file")
        mock_pd.read_parquet.assert_called_once_with("my_input_file")

    def test_split_dataframe(self):
        fake_input_data = [[1, 'product_1', 'image',], [2, 'product_2',], [3, 'product_3', 'image'], [4, 'product_4', ]]
        fake_dataframe = pd.DataFrame(fake_input_data)
        valid_df, invalid_df = split_dataframe(fake_dataframe)
        assert valid_df == 1
        assert invalid_df == 2