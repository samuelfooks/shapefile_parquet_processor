import os
import pandas as pd
import geopandas as gpd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import dask_geopandas as dgpd
import dask.dataframe as dd
import contextily as ctx
from shapely import wkt
import logging


class ShapefileProcessor:

    def __init__(self, shapefile_dir):
        self.shapefile_dir = shapefile_dir

    def list_shapefiles(self, country_codes):
        """
        List all shapefiles that match the provided country codes.

        :param country_codes: List of country code prefixes for filtering.
        :return: List of matching shapefile paths.
        """
        # Assuming the shapefiles are stored in a specific directory
        shapefile_dir = self.shapefile_dir
        shapefile_paths = [
            os.path.join(shapefile_dir, file)
            for file in os.listdir(shapefile_dir)
            if file.endswith('.shp') and any(file.startswith(code) for code in country_codes)
        ]
        return shapefile_paths

    def process_shapefiles(self, shapefile_paths):
        """
        Process the provided shapefiles.

        :param shapefile_paths: List of shapefile paths to process.
        """
        for path in shapefile_paths:
            # Placeholder for actual shapefile processing logic
            logging.info(f'Processing shapefile: {path}')

    def run(self, country_codes: list = ['IT', 'GB', 'FR', 'DK', 'PT']):
        """
        Execute the shapefile processing workflow.

        :param country_codes: List of country code prefixes for filtering.
        """
        shapefile_paths = self.list_shapefiles(country_codes)
        self.process_shapefiles(shapefile_paths)
        logging.info('Shapefile processing completed.')

def read_parquet(parquet_path):
    """Read a parquet file and return a Dask DataFrame."""
    return dd.read_parquet(parquet_path)

def get_unique_gui(dgdf):
    """Get unique values from the 'gui' column."""
    return dgdf['gui'].unique().compute()

def compare_gui_with_shapefiles(shapefile_processor, parquet_path):
    """
    Compare the 'gui' values from the parquet file with the shapefile names.

    :param shapefile_processor: Instance of ShapefileProcessor.
    :param parquet_path: Path to the parquet file.
    """
    dgdf = read_parquet(parquet_path)
    unique_gui = get_unique_gui(dgdf)
    
    country_codes = ['IT', 'GB', 'FR', 'DK', 'PT']  # Define the country codes you want to filter by
    shapefile_processor.run(country_codes)

    # Compare GUIs with shapefiles
    shapefile_paths = shapefile_processor.list_shapefiles(country_codes)
    shapefile_names = [os.path.basename(path) for path in shapefile_paths]

    # for gui in unique_gui:
    #     if pd.isna(gui):
    #         continue
    #     if any(gui in shapefile_name for shapefile_name in shapefile_names):
    #         print(f'{gui} is in the shapefile list')
    #     else:
    #         print(f'{gui} not found in the shapefile list')
    # Reverse check for shapefiles not in GUI
    for shapefile_name in shapefile_names:
        if not any(gui in shapefile_name for gui in unique_gui if not pd.isna(gui)):
            print(f'{shapefile_name} not found in the GUI list')

if __name__ == "__main__":
    wkdir = os.path.dirname(os.path.abspath(__file__))

    shapefile_dir = f'{wkdir}/../src/data/surveymaps'
    PARQUET_PATH = f'{wkdir}/../src/data/sbh_survey_parquet_test'
    
    shapefile_processor = ShapefileProcessor(shapefile_dir)
    compare_gui_with_shapefiles(shapefile_processor, PARQUET_PATH)
