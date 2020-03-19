import unittest
from unittest import mock
from unittest.mock import MagicMock

import pytest

from transformater.transform import run


class TestTransform(unittest.TestCase):
    @mock.patch("transformater.transform.download_file_from_s3_public_bucket")
    @mock.patch("transformater.transform.csv_file_to_parquet")
    @mock.patch("transformater.transform.split_dataframe_by_missing_key")
    @mock.patch("transformater.transform.pd")
    def test_run_sucess(
        self,
        mock_pandas,
        mock_split_dataframe_by_missing_key,
        mock_csv_file_to_parquet,
        mock_download_file_from_s3_public_bucket,
    ):
        mock_df = MagicMock()
        mock_df.shape = [4, 2]
        mock_pandas.read_parquet.return_value = mock_df
        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_df1.shape = [1, 2]
        mock_df2.shape = [3, 2]

        mock_split_dataframe_by_missing_key.return_value = mock_df1, mock_df2
        run(
            s3_bucket="my_s3_bucket",
            s3_object="my_s3_object",
            local_file_name="my_local_file_name",
            splitting_key="my_splitting_key",
        )
        mock_download_file_from_s3_public_bucket.assert_called_once_with(
            bucket="my_s3_bucket",
            object="my_s3_object",
            output_file="my_local_file_name.csv",
        )
        mock_csv_file_to_parquet.assert_called_once_with(
            input_file="my_local_file_name.csv",
            output_file="my_local_file_name.parquet",
        )
        mock_df1.to_parquet.assert_called_once_with(
            "product_with_my_splitting_key.parquet"
        )
        mock_df2.to_parquet.assert_called_once_with(
            "product_without_my_splitting_key.parquet"
        )

    @mock.patch("transformater.transform.download_file_from_s3_public_bucket")
    @mock.patch("transformater.transform.csv_file_to_parquet")
    @mock.patch("transformater.transform.split_dataframe_by_missing_key")
    @mock.patch("transformater.transform.pd")
    def test_run_fail(
        self,
        mock_pandas,
        mock_split_dataframe_by_missing_key,
        mock_csv_file_to_parquet,
        mock_download_file_from_s3_public_bucket,
    ):
        mock_df = MagicMock()
        mock_df.shape = [4, 2]
        mock_pandas.read_parquet.return_value = mock_df
        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_df1.shape = [1, 2]
        mock_df2.shape = [2, 2]

        mock_split_dataframe_by_missing_key.return_value = mock_df1, mock_df2
        with pytest.raises(ValueError):
            run(
                s3_bucket="my_s3_bucket",
                s3_object="my_s3_object",
                local_file_name="my_local_file_name",
                splitting_key="my_splitting_key",
            )
