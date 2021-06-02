"""
Function to follow USGS bulk files release and show some information about them
"""

import logging
import time

import pandas as pd

from landsat_scenes_sync.variables import BASE_BULK_CSV_URL
from utils.sync_utils import (
    download_file_to_tmp,
    time_process,
)


def check_files_dates(file_name: str):
    """
    Function to follow USGS bulk files release and show some information about them

    :param file_name: (String) File name which will be downloaded
    :return: None. Process send information to the queue
    """
    try:
        start_timer = time.time()

        logging.info(f"Starting Check Files")

        # Download GZIP file
        file_path = download_file_to_tmp(url=BASE_BULK_CSV_URL, file_name=file_name)

        # Read file and retrieve the Display ids
        c = pd.read_csv(file_path)
        pd.set_option("display.max_columns", None)

        print("\n {c.head()}")

        c["prod_date"] = pd.to_datetime(c["Date Product Generated L2"])

        print(f"MAX Date: \n {c['prod_date'].max()}")

        grouped = (
            c.groupby(["prod_date"])["Landsat Product Identifier L2"]
            .count()
            .sort_index()
        )

        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            # more options can be specified also
            print(f"Grouped Generated Date: \n {grouped}")

        logging.info(
            f"File {file_name} processed and sent in {time_process(start=start_timer)}"
        )

        logging.info(f"Whole process finished successfully ;)")

    except Exception as error:
        logging.error(error)
        raise error
