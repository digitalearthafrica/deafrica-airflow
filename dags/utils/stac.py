import datetime
import logging

import dateutil
import rasterio
from pystac import Item, Link, MediaType
from rasterio import RasterioIOError

try:
    from pathlib import Path
except ImportError:  # pragma: no cover
    class Path:
        pass


def _parse_date(in_date: str) -> datetime.datetime:
    """
    Try to parse a date and return it as a datetime object with no timezone
    """
    dt = dateutil.parser.parse(in_date)
    return dt.replace(tzinfo=datetime.timezone.utc)


def transform_stac_to_stac(
        item: Item,
        blue_asset,
        enable_proj: bool = True,
        self_link: str = None,
        source_link: str = None
) -> Item:
    """
    Handle a 0.7.0 item and convert it to a 1.0.0.beta2 item.
    """
    logging.info(f'transform_stac_to_stac START ')
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
    logging.info(f'After links ')
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
    logging.info(f'After ITEMS ')

    if enable_proj:
        logging.info(f'After enable_proj ')
        try:
            # If we can load the blue band, use it to add proj information
            # blue_asset = item.assets["SR_B2.TIF"]
            # blue = rasterio.open(blue_asset.href)
            if not isinstance(blue_asset, str):
                if (
                        not (
                                hasattr(blue_asset, 'read')
                                or hasattr(blue_asset, 'write')
                                or isinstance(blue_asset, Path)
                        )
                ):
                    raise TypeError("invalid path or file: blue_asset")
            blue = rasterio.open(blue_asset)
            logging.info(f'After blue')
            shape = [blue.height, blue.width]
            logging.info(f'After shape')
            transform = blue.transform
            logging.info(f'After Transform')
            crs = blue.crs.to_epsg()
            logging.info(f'After crs')

            # Now we have the info, we can make the fields
            item.ext.enable("projection")
            item.ext.projection.epsg = crs
            logging.info(f'After ITEM')

            for name, asset in item.assets.items():
                if asset.media_type == "image/vnd.stac.geotiff; cloud-optimized=true":
                    item.ext.projection.set_transform(transform, asset=asset)
                    item.ext.projection.set_shape(shape, asset=asset)
                    asset.media_type = MediaType.COG

            logging.info(f'After ')
        except RasterioIOError:
            logging.info(f'error ')
            raise Exception("Failed to load blue band, so not handling proj fields")

    # Remove .TIF from asset names
    new_assets = {}

    for name, asset in item.assets.items():
        new_name = name.replace(".TIF", "")
        new_assets[new_name] = asset
    item.assets = new_assets
    logging.info(f'END OF FUNCTION')
    return item
