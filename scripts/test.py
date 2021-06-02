"""Just tests, must be deleted"""
import datetime
import json
import os
import unittest
from pathlib import Path
from urllib.parse import urlparse

import rasterio
import requests
from pystac import Item
from rasterio import RasterioIOError

from utils.stactools_mock import transform_stac_to_stac
from utils.sync_utils import read_big_csv_files_from_gzip, read_csv_from_gzip


def count_scenes():
    file_path = Path(
        os.path.join("C:/Users/cario/Downloads/", "LANDSAT_OT_C2_L2.csv.gz")
    )
    url = urlparse(
        f"{'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/'}{'LANDSAT_OT_C2_L2.csv.gz'}"
    )
    downloaded = requests.get(url.geturl(), stream=True)
    file_path.write_bytes(downloaded.content)

    africa_pathrows = read_csv_from_gzip(
        file_path="https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-usgs-pathrows.csv.gz"
    )

    count = 0
    for row in read_big_csv_files_from_gzip(file_path):
        if (
            # Filter to skip all LANDSAT_4
            row.get("Satellite")
            and row["Satellite"] != "LANDSAT_4"
            # Filter to get just day
            and (
                row.get("Day/Night Indicator")
                and row["Day/Night Indicator"].upper() == "DAY"
            )
            # Filter to get just from Africa
            and (
                row.get("WRS Path")
                and row.get("WRS Row")
                and int(f"{row['WRS Path']}{row['WRS Row']}") in africa_pathrows
            )
        ):
            count += 1
    print(f"Number of scenes {count}")

    sec_transfer = 90 * count
    transfer_time = datetime.timedelta(seconds=sec_transfer)

    print(f"Time to transfer {str(transfer_time)}")


def convert(json_path: str):
    try:
        with open(json_path) as f:
            item = Item.from_dict(json.load(f))
            print(item)
            return item
    except Exception as error:
        print(error)


def compare(landsat5, landsat7, landsat8):
    item5 = convert(landsat5)
    item7 = convert(landsat7)
    item8 = convert(landsat8)

    compare_5_7 = [k for k in item5.assets.keys() if k in item7.assets.keys()]
    print(f"Assets that are in 7 and 5 {compare_5_7}")

    compare_5_7_8 = [k for k in item8.assets.keys() if k in compare_5_7]

    print(f"Assets that are in 7 and 5 and 8 {compare_5_7_8}")


def rasterio_test(item: Item):
    item.ext.enable("projection")

    with rasterio.Env(
        aws_unsigned=True,
    ):
        for name, asset in item.assets.items():
            if "geotiff" in asset.media_type:
                try:
                    href = asset.href.replace(
                        "s3://deafrica-landsat-dev/",
                        "https://deafrica-landsat-dev.s3.af-south-1.amazonaws.com/",
                    ).replace(
                        "https://landsatlook.usgs.gov/data/",
                        "https://deafrica-landsat-dev.s3.af-south-1.amazonaws.com/",
                    )
                    # ).replace(
                    #     "https://landsatlook.usgs.gov/data/",
                    #     "https://deafrica-landsat-dev.s3.af-south-1.amazonaws.com/",
                    # )
                    with rasterio.open(href) as opened_asset:
                        shape = opened_asset.shape
                        transform = opened_asset.transform
                        crs = opened_asset.crs.to_epsg()
                        print(f"success {href}")

                    item.ext.projection.set_transform(transform, asset=asset)
                    item.ext.projection.set_shape(shape, asset=asset)

                except RasterioIOError as error:
                    # print(f"Failed {href}")
                    # print(f"error {error}")
                    # print(RasterioIOError == type(error))
                    raise error
                    # raise Exception('test')
                    # continue
    return item


class class_test(unittest.TestCase):
    @unittest.skip
    def test_replace_extensions(self):
        json_landsat82 = os.path.join(
            "C:/Users/cario/work/deafrica-airflow/scripts/", "landsat8.json"
        )
        item = convert(json_path=json_landsat82)
        print(item.stac_extensions)

        item.stac_extensions = list(
            filter(
                lambda x: ("https://landsat.usgs.gov" not in x), item.stac_extensions
            )
        )
        print(item.stac_extensions)

    @unittest.skip
    def test_rasterio(self):
        json_landsat82 = os.path.join(
            "C:/Users/cario/work/deafrica-airflow/scripts/", "landsat8.json"
        )
        with self.assertRaises(RasterioIOError):
            rasterio_test(convert(json_path=json_landsat82))

    def test_transform_static_stac_missing_asset_b2_b10(self):
        """It has to be able to gather the right information from other geotiff files"""

        json_landsat8 = os.path.join(
            "C:/Users/cario/work/deafrica-airflow/scripts/", "landsat8.json"
        )

        item = convert(json_path=json_landsat8)

        if item.assets.get("SR_B2.TIF"):
            item.assets.pop("SR_B2.TIF")

        if item.assets.get("ST_B10.TIF"):
            item.assets.pop("ST_B10.TIF")

        for asset in item.assets.values():
            if "geotiff" in asset.media_type:
                asset.href = "C:/Users/cario/work/deafrica-airflow/scripts/LC08_L2SR_081119_20200101_20200823_02_T2_SR_B2_small.TIF"

        item = transform_stac_to_stac(item)
        item.validate()


if __name__ == "__main__":
    json_file = os.path.join(
        "C:/Users/cario/work/deafrica-airflow/scripts/", "test.json"
    )
    json_landsat5 = os.path.join(
        "C:/Users/cario/work/deafrica-airflow/scripts/", "landsat5.json"
    )
    json_landsat7 = os.path.join(
        "C:/Users/cario/work/deafrica-airflow/scripts/", "landsat7.json"
    )
    json_landsat8 = os.path.join(
        "C:/Users/cario/work/deafrica-airflow/scripts/", "landsat8.json"
    )

    # get_queue_attributes_test()
    # convert(json_path=json_file)
    # compare(json_landsat5, json_landsat7, json_landsat8)
    # rasterio_test(json_file)
    unittest.main()
    # count_scenes()
