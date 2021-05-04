"""Just tests, must be deleted"""
import json
import os
import unittest

from pystac import Item
import rasterio
from rasterio import RasterioIOError


def convert(json_path: str):
    try:
        with open(json_path) as f:
            item = Item.from_dict(json.load(f))
            print(item)
            print(item.to_dict())
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


def rasterio_test(json_path):

    print(json_path)
    item = convert(json_path=json_path)

    item.ext.enable("projection")

    with rasterio.Env(
        aws_unsigned=True,
        # CURL_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt",
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
                    with rasterio.open(href) as opened_asset:
                        shape = opened_asset.shape
                        transform = opened_asset.transform
                        crs = opened_asset.crs.to_epsg()
                        print(f"success {href}")

                    try:
                        item.ext.projection.set_transform(transform, asset=asset)
                        item.ext.projection.set_shape(shape, asset=asset)
                        print("WORKED")
                    except Exception:
                        continue

                except RasterioIOError as error:

                    # print(f"Failed {href}")
                    # print(f"error {error}")
                    # print(RasterioIOError == type(error))
                    raise error
                    # raise Exception('test')
                    # continue


class class_test(unittest.TestCase):
    def test_upper(self):
        self.assertEqual("foo".upper(), "FOO")

    def test_rasterio(self):
        json_landsat82 = os.path.join(
            "C:/Users/cario/work/deafrica-airflow/scripts/", "landsat8.json"
        )
        with self.assertRaises(RasterioIOError):
            rasterio_test(json_landsat82)

        # try:
        #     rasterio_test(json_landsat82)
        #
        # except RasterioIOError:
        #     self.assertTrue(True, 'It was expected a RasterioIOError')
        #
        # except Exception as e:
        #     self.assertTrue(False, 'It was expected a RasterioIOError')


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

    # convert(json_file)
    # compare(json_landsat5, json_landsat7, json_landsat8)
    rasterio_test(json_file)
    # unittest.main()
