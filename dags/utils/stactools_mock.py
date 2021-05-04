""" Mock transform_stac_to_stac while stacktools library isn't ready"""
import rasterio
from pystac import Item, Link, MediaType, STACError
from rasterio import RasterioIOError


def transform_stac_to_stac(
    item: Item, enable_proj: bool = True, self_link: str = None, source_link: str = None
) -> Item:
    """
    Handle a 0.7.0 item and convert it to a 1.0.0.beta2 item.
    If `enable_proj` is true, the assets' geotiff files must be accessible.
    """
    # Clear hierarchical links
    item.set_parent(None)
    item.set_root(None)

    # Remove USGS extension and add back eo
    item.ext.enable("eo")

    # Add and update links
    if self_link:
        item.links.append(Link(rel="self", target=self_link))
    if source_link:
        item.links.append(
            Link(rel="derived_from", target=source_link, media_type="application/json")
        )

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

        shape = None
        transform = None
        crs = None
        for name, asset in item.assets.items():
            if "geotiff" in asset.media_type:
                # retrieve shape, transform and crs from the first geotiff file among the assets
                if not shape:
                    try:
                        with rasterio.open(asset.href) as opened_asset:
                            shape = opened_asset.shape
                            transform = opened_asset.transform
                            crs = opened_asset.crs.to_epsg()
                            # Check to ensure that all information is present
                            if not shape or not transform or not crs:
                                raise STACError(
                                    f"Failed setting shape, transform and csr from {asset.href}"
                                )

                    except RasterioIOError as io_error:
                        raise STACError(
                            f"Failed loading geotiff, so not handling proj fields, {io_error}"
                        )

                item.ext.projection.set_transform(transform, asset=asset)
                item.ext.projection.set_shape(shape, asset=asset)
                asset.media_type = MediaType.COG

        # Now we have the info, we can make the fields
        item.ext.enable("projection")
        item.ext.projection.epsg = crs

    # Remove .TIF from asset names
    item.assets = {
        name.replace(".TIF", ""): asset for name, asset in item.assets.items()
    }

    return item
