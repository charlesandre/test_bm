import unittest
from unittest import mock

import pandas as pd
from pandas._testing import assert_frame_equal

from transformater.utils import (UNSIGNED, csv_file_to_parquet,
                                 download_file_from_s3_public_bucket,
                                 parquet_to_df, split_dataframe_by_missing_key)


class TestUtils(unittest.TestCase):
    @mock.patch("transformater.utils.boto3")
    @mock.patch("transformater.utils.Config")
    def test_download_file_from_s3_public_bucket(self, mock_config, mock_boto3):
        download_file_from_s3_public_bucket(
            "my_bucket", "my_object", "my_destination_name"
        )
        mock_config.assert_called_once_with(signature_version=UNSIGNED)
        mock_boto3.client.assert_called_once_with("s3", config=mock_config.return_value)
        mock_boto3.client.return_value.download_file.assert_called_once_with(
            "my_bucket", "my_object", "my_destination_name"
        )

    @mock.patch("transformater.utils.pd")
    def test_csv_file_to_parquet(self, mock_pd):
        csv_file_to_parquet("my_input_file", "my_output_file")
        mock_pd.read_csv.assert_called_once_with("my_input_file")
        mock_pd.read_csv.return_value.to_parquet.assert_called_once_with(
            "my_output_file"
        )

    @mock.patch("transformater.utils.pd")
    def test_parquet_to_df(self, mock_pd):
        parquet_to_df("my_input_file")
        mock_pd.read_parquet.assert_called_once_with("my_input_file")

    def test_split_dataframe(self):
        fake_dataframe = pd.DataFrame(
            [
                [1, "product_1", "img",],
                [2, "product_2",],
                [3, "product_3", "img"],
                [4, "product_4",],
            ],
            columns=["product_id", "product_name", "image"],
        )

        expected_valid_df = pd.DataFrame(
            [[1, "product_1", "img",], [3, "product_3", "img"]],
            columns=["product_id", "product_name", "image"],
        )
        expected_archive_df = pd.DataFrame(
            [[2, "product_2", None], [4, "product_4", None]],
            columns=["product_id", "product_name", "image"],
        )

        valid_df, archive_df = split_dataframe_by_missing_key(
            df=fake_dataframe, key="image"
        )

        assert_frame_equal(
            valid_df.reset_index(drop=True), expected_valid_df.reset_index(drop=True)
        )
        assert_frame_equal(
            archive_df.reset_index(drop=True),
            expected_archive_df.reset_index(drop=True),
        )
