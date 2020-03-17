import boto3
from botocore import UNSIGNED
from botocore.client import Config

import pandas as pd


def download_file_from_s3_public_bucket(bucket, object, output_file):
    """
        This function download an object from a public S3 and save it given a destination name

        :param bucket: name of the S3 public bucket
        :type bucket: str
        :param object: object to download from S3 (ex: path/to/object)
        :type object: str
        :param output_file: name of the file to store locally
        :type output_file: str
    """
    botocore_config = Config(signature_version=UNSIGNED)
    s3_client = boto3.client('s3', config=botocore_config)
    s3_client.download_file(bucket, object, output_file)
    

def csv_file_to_parquet(input_file, output_file):
    """
        This function transform a csv file into a parquet file using pandas

        :param input_file: csv file to transform
        :type input_file: str
        :param output_file: name of the output parquet file
        :type output_file: str
    """
    df = pd.read_csv(input_file)
    df.to_parquet(output_file)

def parquet_to_df(input_file):
    df = pd.read_parquet(input_file)

def split_dataframe(df):
    return 1, 2