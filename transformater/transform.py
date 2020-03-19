import logging

import pandas as pd

from transformater.utils import (csv_file_to_parquet,
                                 download_file_from_s3_public_bucket,
                                 split_dataframe_by_missing_key)


def run(s3_bucket, s3_object, local_file_name, splitting_key):
    logging.info("Download file from S3 locally..")
    download_file_from_s3_public_bucket(
        bucket=s3_bucket, object=s3_object, output_file=f"{local_file_name}.csv",
    )
    logging.info("Download DONE")

    logging.info("Convert file to csv .. ")
    csv_file_to_parquet(
        input_file=f"{local_file_name}.csv", output_file=f"{local_file_name}.parquet"
    )
    logging.info("Convert to parquet DONE")

    logging.info("Read parquet file with pandas..")
    df = pd.read_parquet(f"{local_file_name}.parquet")

    logging.info("Split dataframe.. ")
    total_rows = df.shape[0]
    df_with_img, df_without_img = split_dataframe_by_missing_key(df=df, key="image")
    if total_rows != df_with_img.shape[0] + df_without_img.shape[0]:
        logging.error("Lost some rows when splitting dataframe")
        raise ValueError("Lost some rows during splitting of the dataframe")

    logging.info("Splitting is donne, saving files ..")
    df_with_img.to_parquet(f"product_with_{splitting_key}.parquet")
    df_without_img.to_parquet(f"product_without_{splitting_key}.parquet")


if __name__ == "__main__":
    run(s3_bucket="backmarket-data-jobs", s3_object="data/product_catalog.csv", local_file_name="products", splitting_key="image")
