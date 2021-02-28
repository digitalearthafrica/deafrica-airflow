"""
JUST A TEST, IT'LL BE DELETED JUST AFTER RUNNING OK IN DEV
"""

import json
from odc.index.stac import stac_transform

DATA = {'type': 'Feature', 'stac_version': '1.0.0-beta.2', 'id': 'LE07_L2SP_180050_20210101_20210127_02_T1',
        'properties': {'collection': 'landsat-c2l2-sr', 'eo:gsd': 30, 'eo:platform': 'LANDSAT_7',
                       'datetime': '2021-01-01T00:00:00Z', 'eo:cloud_cover': 0, 'eo:sun_azimuth': 136.08109634,
                       'eo:sun_elevation': 38.52047067, 'landsat:cloud_cover_land': 0, 'landsat:wrs_type': '2',
                       'landsat:wrs_path': '180', 'landsat:wrs_row': '050', 'landsat:scene_id': 'LE71800502021001SG100',
                       'landsat:collection_category': 'T1', 'landsat:collection_number': '02',
                       'eo:bands': [{'center_wavelength': 0.48, 'name': 'SR_B1', 'gsd': 30, 'common_name': 'blue'},
                                    {'center_wavelength': 0.56, 'name': 'SR_B2', 'gsd': 30, 'common_name': 'green'},
                                    {'center_wavelength': 0.65, 'name': 'SR_B3', 'gsd': 30, 'common_name': 'red'},
                                    {'center_wavelength': 0.86, 'name': 'SR_B4', 'gsd': 30, 'common_name': 'nir08'},
                                    {'center_wavelength': 1.6, 'name': 'SR_B5', 'gsd': 30, 'common_name': 'swir16'},
                                    {'center_wavelength': 2.2, 'name': 'SR_B7', 'gsd': 30, 'common_name': 'swir22'}],
                       'odc:product': 'https://explorer-af.digitalearth.africa/product/ls7_c2l2',
                       'constellation': 'Landsat', 'instruments': ['etm'], 'view:off_nadir': 0, 'proj:epsg': 32634},
        'geometry': {'type': 'Polygon', 'coordinates': [
            [[21.21326870738437, 15.398599927181248], [23.00693005432424, 15.132387218303997],
             [22.641870298650964, 13.527393449345057], [20.8661777231609, 13.78366530386187],
             [21.21326870738437, 15.398599927181248]]]}, 'links': [{'rel': 'self',
                                                                    'href': 's3://deafrica-landsat-dev/collections/landsat-c2l2-sr/items/LE07_L2SP_180050_20210101_20210127_02_T1'},
                                                                   {'rel': 'derived_from',
                                                                    'href': 'https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-sr/items/LE07_L2SP_180050_20210101_20210127_02_T1',
                                                                    'type': 'application/json'}], 'assets': {
        'thumbnail': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_thumb_small.jpeg',
            'type': 'image/jpeg', 'title': 'Thumbnail image'}, 'reduced_resolution_browse': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_thumb_large.jpeg',
            'type': 'image/jpeg', 'title': 'Reduced resolution browse image'}, 'index': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1',
            'type': 'text/html', 'title': 'HTML index page'}, 'ST_B6': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ST_B6.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
            'title': 'Surface Temperature Band (B6)',
            'description': 'Landsat Collection 2 Level-2 Surface Temperature Band (B6) Surface Temperature Product',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'ST_ATRAN': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ST_ATRAN.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
            'title': 'Atmospheric Transmittance Band',
            'description': 'Landsat Collection 2 Level-2 Atmospheric Transmittance Band Surface Temperature Product',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'ST_CDIST': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ST_CDIST.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Cloud Distance Band',
            'description': 'Landsat Collection 2 Level-2 Cloud Distance Band Surface Temperature Product',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'ST_DRAD': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ST_DRAD.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Downwelled Radiance Band',
            'description': 'Landsat Collection 2 Level-2 Downwelled Radiance Band Surface Temperature Product',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'ST_URAD': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ST_URAD.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Upwelled Radiance Band',
            'description': 'Landsat Collection 2 Level-2 Upwelled Radiance Band Surface Temperature Product',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'ST_TRAD': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ST_TRAD.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Thermal Radiance Band',
            'description': 'Landsat Collection 2 Level-2 Thermal Radiance Band Surface Temperature Product',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'ST_EMIS': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ST_EMIS.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Emissivity Band',
            'description': 'Landsat Collection 2 Level-2 Emissivity Band Surface Temperature Product',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'ST_EMSD': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ST_EMSD.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
            'title': 'Emissivity Standard Deviation Band',
            'description': 'Landsat Collection 2 Level-2 Emissivity Standard Deviation Band Surface Temperature Product',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'ST_QA': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ST_QA.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
            'title': 'Surface Temperature Quality Assessment Band',
            'description': 'Landsat Collection 2 Level-2 Surface Temperature Band Surface Temperature Product',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'ANG.txt': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_ANG.txt',
            'type': 'text/plain', 'title': 'Angle Coefficients File',
            'description': 'Collection 2 Level-2 Angle Coefficients File (ANG) Surface Reflectance'}, 'MTL.txt': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_MTL.txt',
            'type': 'text/plain', 'title': 'Product Metadata File',
            'description': 'Collection 2 Level-2 Product Metadata File (MTL) Surface Reflectance'}, 'MTL.xml': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_MTL.xml',
            'type': 'application/xml', 'title': 'Product Metadata File (xml)',
            'description': 'Collection 2 Level-1 Product Metadata File (xml) Surface Reflectance'}, 'MTL.json': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_MTL.json',
            'type': 'application/json', 'title': 'Product Metadata File (json)',
            'description': 'Collection 2 Level-2 Product Metadata File (json) Surface Reflectance'}, 'QA_PIXEL': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_QA_PIXEL.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
            'title': 'Pixel Quality Assessment Band',
            'description': 'Collection 2 Level-2 Pixel Quality Assessment Band Surface Reflectance',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'QA_RADSAT': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_QA_RADSAT.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
            'title': 'Radiometric Saturation Quality Assessment Band',
            'description': 'Collection 2 Level-2 Radiometric Saturation Quality Assessment Band Surface Reflectance',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'SR_B2': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_SR_B2.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Green Band (B2)',
            'description': 'Collection 2 Level-2 Green Band (B2) Surface Reflectance', 'eo:bands': [1],
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'SR_B7': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_SR_B7.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
            'title': 'Short-wave Infrared Band 2.2 (B7)',
            'description': 'Collection 2 Level-2 Short-wave Infrared Band 2.2 (B7) Surface Reflectance',
            'eo:bands': [5], 'proj:transform': (30.0, 0.0, 478785.0,
                                                      0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'SR_B4': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_SR_B4.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Near Infrared Band 0.8 (B4)',
            'description': 'Collection 2 Level-2 Near Infrared Band 0.8 (B4) Surface Reflectance', 'eo:bands': [3],
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'SR_CLOUD_QA': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_SR_CLOUD_QA.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Cloud Quality Analysis Band',
            'description': 'Collection 2 Level-2 Cloud Quality Opacity Band Surface Reflectance',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'SR_B3': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_SR_B3.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Red Band (B3)',
            'description': 'Collection 2 Level-2 Red Band (B3) Surface Reflectance', 'eo:bands': [2],
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'SR_B5': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_SR_B5.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized',
            'title': 'Short-wave Infrared Band 1.6 (B5)',
            'description': 'Collection 2 Level-2 Short-wave Infrared Band 1.6 (B6) Surface Reflectance',
            'eo:bands': [4], 'proj:transform': (30.0, 0.0, 478785.0,
                                                      0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]},
        'SR_ATMOS_OPACITY': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_SR_ATMOS_OPACITY.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'Atmospheric Opacity Band',
            'description': 'Collection 2 Level-2 Atmospheric Opacity Band Surface Reflectance',
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}, 'SR_B1': {
            'href': 's3://deafrica-landsat-dev/collection02/level-2/standard/etm/2021/180/050/LE07_L2SP_180050_20210101_20210127_02_T1/LE07_L2SP_180050_20210101_20210127_02_T1_SR_B1.TIF',
            'type': 'image/tiff; application=geotiff; profile=cloud-optimized', 'title': 'BLue Band (B1)',
            'description': 'Collection 2 Level-2 Blue Band (B1) Surface Reflectance', 'eo:bands': [0],
            'proj:transform': (30.0, 0.0, 478785.0,
                                     0.0, -30.0, 1703715.0), 'proj:shape': [6961, 8071]}},
        'bbox': [20.8661777231609, 13.527393449345057, 23.00693005432424, 15.398599927181248],
        'stac_extensions': ['eo', 'https://landsat.usgs.gov/stac/landsat-extension/schema.json', 'view', 'projection'],
        'collection': 'landsat-c2l2-sr', 'description': 'Landsat Collection 2 Level-2 Surface Reflectance Product'}


# TODO Remove the url and check why the transform isn't adding the proj:epsg property and extension projection


def test_transformation():
    json_data = json.dumps(DATA)
    print(stac_transform(DATA))


if __name__ == "__main__":
    test_transformation()