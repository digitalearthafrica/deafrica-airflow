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

    # def test_rasterio(self):
    #     json_landsat82 = os.path.join(
    #         "C:/Users/cario/work/deafrica-airflow/scripts/", "landsat8.json"
    #     )
    #     with self.assertRaises(RasterioIOError):
    #         rasterio_test(
    #             convert(json_path=json_landsat82)
    #         )
    #
    # def test_transform_static_stac_missing_asset_b2_b10(self):
    #     """It has to be able to gather the right information from other geotiff files"""
    #
    #     json_landsat82 = os.path.join(
    #         "C:/Users/cario/work/deafrica-airflow/scripts/", "landsat8.json"
    #     )
    #
    #     item = convert(json_path=json_landsat82)
    #
    #     if item.assets.get("SR_B2.TIF"):
    #         item.assets.pop("SR_B2.TIF")
    #
    #     if item.assets.get("ST_B10.TIF"):
    #         item.assets.pop("ST_B10.TIF")
    #
    #     item = rasterio_test(item=item)
    #     item.validate()


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
    # rasterio_test(json_file)
    unittest.main()
