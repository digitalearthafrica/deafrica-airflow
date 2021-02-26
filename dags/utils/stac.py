"""
Python test for the library, it'll be deleted once the process is working perfectly in DEV
"""

import logging
import rasterio
from pystac import Item, Link, MediaType


def transform_stac_to_stac(item: Item,
                           enable_proj: bool = True,
                           self_link: str = None,
                           source_link: str = None) -> Item:
    """
    Handle a 0.7.0 item and convert it to a 1.0.0.beta2 item.
    """
    # Remove USGS extension and add back eo
    item.ext.enable("eo")

    # Add and update links
    item.links = []
    if self_link:
        item.links.append(Link(rel="self", target=self_link))
    if source_link:
        item.links.append(
            Link(rel="derived_from",
                 target=source_link,
                 media_type="application/json"))

    # Add some common fields
    item.common_metadata.constellation = "Landsat"
    item.common_metadata.instruments = [
        i.lower() for i in item.properties["eo:instrument"].split("_")
    ]
    del item.properties["eo:instrument"]

    # Handle view extension
    item.ext.enable("view")
    item.ext.view.off_nadir = item.properties["eo:off_nadir"]
    del item.properties["eo:off_nadir"]

    if enable_proj:
        logging.info(f'enable_proj {enable_proj}')
        try:
            # If we can load the blue band, use it to add proj information
            blue_asset = item.assets["SR_B2.TIF"]
            logging.info(f'blue_asset.href {blue_asset.href}')
            # blue = rasterio.open('/home/LE07_L2SP_180063_20210101_20210127_02_T2_SR_B2.TIF')
            blue = rasterio.open(blue_asset.href)
            logging.info(f'blue {blue}')
            shape = [blue.height, blue.width]
            transform = blue.transform
            crs = blue.crs.to_epsg()

            # Now we have the info, we can make the fields
            item.ext.enable("projection")
            item.ext.projection.epsg = crs

            logging.info(f'Just before FOR {item.assets.items()}')

            for name, asset in item.assets.items():
                if asset.media_type == "image/vnd.stac.geotiff; cloud-optimized=true":
                    item.ext.projection.set_transform(transform, asset=asset)
                    item.ext.projection.set_shape(shape, asset=asset)
                    asset.media_type = MediaType.COG
                    logging.info(f'asset.media_type inside the if {asset.media_type}')

        # except RasterioIOError:
        #     raise Exception("Failed to load blue band, so not handling proj fields")
        except Exception as e:
            raise e

    # Remove .TIF from asset names
    new_assets = {}

    for name, asset in item.assets.items():
        new_name = name.replace(".TIF", "")
        new_assets[new_name] = asset
    item.assets = new_assets

    return item