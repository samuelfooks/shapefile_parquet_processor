# src/shapefile_processor/shapefile_to_gdf.py
import logging
import pyogrio
import geopandas as gpd

def shapefile_to_gdf(file_path: str, crs: str = 'EPSG:4326') -> gpd.GeoDataFrame:
    """
    Read a shapefile into a GeoDataFrame.

    :param file_path: Path to the shapefile.
    :param crs: Desired Coordinate Reference System.
    :return: GeoDataFrame.
    """
    try:
        gdf = pyogrio.read_dataframe(file_path)
        if gdf is None or gdf.empty:
            logging.error(f'Empty or invalid GeoDataFrame for file {file_path}')
            return None
        # Ensure CRS is correct
        if gdf.crs != crs:
            logging.info(f'Converting CRS for {file_path} to {crs}')
            gdf = gdf.to_crs(crs)
        return gdf
    except Exception as e:
        logging.error(f'Error reading shapefile {file_path}: {e}')
        return None
