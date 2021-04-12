"""
Utils to support the Landsat process
"""
import csv
import gzip
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import pandas as pd
import requests


def test_http_return(returned):
    """
    Test API response
    :param returned:
    :return:
    """
    if hasattr(returned, "status_code") and returned.status_code != 200:
        url = returned.url if hasattr(returned, "url") else "Not informed"
        content = returned.content if hasattr(returned, "content") else "Not informed"
        text = returned.text if hasattr(returned, "text") else "Not informed"
        status_code = (
            returned.status_code if hasattr(returned, "status_code") else "Not informed"
        )
        reason = returned.reason if hasattr(returned, "reason") else "Not informed"
        raise Exception(
            f"API return is not 200: \n"
            f"-url: {url} \n"
            f"-content: {content} \n"
            f"-text: {text} \n"
            f"-status_code: {status_code} \n"
            f"-reason: {reason} \n"
        )


def request_url(url: str, params=None):
    """
    Function to request an url
    :param url:
    :param params: (dict)
    :return:
    """
    if params is None:
        params = {}
    resp = requests.get(url=url, params=params)
    # Check return 200
    test_http_return(resp)
    logging.info(f"requested {url}")
    return json.loads(resp.content)


def convert_str_to_date(date: str):
    """
    Function to convert a date in a string format into a datetime YYYY/MM/DD.

    :param date: (str) Date in a string format
    :return: (datetime) return datetime of a string date. The time will always be 0.
    """
    try:
        return datetime.strptime(date, "%Y/%M/%d").date()
    except ValueError:
        try:
            return datetime.strptime(date, "%Y-%M-%d").date()
        except ValueError as error:
            raise error


def read_csv_from_gzip(file_path):
    """
    Function to read inner csv file in a GZIP file and return set of values.
    Warning: Function uses Pandas, so it will load everything in memory.

    :param file_path: (str)
    :return: (set) values
    """

    return set(
        pd.read_csv(
            file_path,
            header=None,
        ).values.ravel()
    )


def read_big_csv_files_from_gzip(file_path: Path):
    """
    Function to read inner csv file in a GZIP file and return set of values.
    :param file_path: (Path)
    :return:
    """
    with gzip.open(file_path, "rt") as csv_file:
        for row in csv.DictReader(csv_file):
            yield row


def download_file_to_tmp(url: str, file_name: str, always_return_path: bool = True):
    """
    Function to check if a specific file is already downloaded based on its size, if not downloaded,
    it will download the file from the informed server.
    The file will be saved in the local machine/container under the /tmp/ folder, so the OS will delete that accordingly
    with its pre-defined configurations.
    Warning: The server shall have enough free storage.

    :param url:(String) URL path for the file server
    :param file_name: (String) File name which will be downloaded
    :param always_return_path:(bool) Returns the path even if already updated
    :return: (String) File path where it was downloaded. Hardcoded for /tmp/
    """

    logging.info("Start downloading files")

    url = urlparse(f"{url}{file_name}")
    file_path = Path(f"/tmp/{file_name}")

    # check if file exists and comparing size against cloud file
    if file_path.exists():

        logging.info(f"File already found on {file_path}")

        file_size = file_path.stat().st_size
        head = requests.head(url.geturl())

        if hasattr(head, "headers") and head.headers.get("Content-Length"):
            server_file_size = head.headers["Content-Length"]
            logging.info(
                f"Comparing sizes between local saved file and server hosted file,"
                f" local file size : {file_size} server file size: {server_file_size}"
            )

            if int(file_size) == int(server_file_size):
                logging.info("Already updated!!")
                return file_path if always_return_path else ""

    logging.info(f"Downloading file {file_name} to {file_path}")
    downloaded = requests.get(url.geturl(), stream=True)
    file_path.write_bytes(downloaded.content)

    logging.info(f"{file_name} Downloaded!")
    return file_path


def time_process(start: float):
    """
    Times the process
    :param start:
    :return:
    """
    t_sec = round(time.time() - start)
    (t_min, t_sec) = divmod(t_sec, 60)
    (t_hour, t_min) = divmod(t_min, 60)

    return f"{t_hour} hour: {t_min} min: {t_sec} sec"
