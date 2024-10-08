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
def make_check_logger(wkdir):
    """
    Create a logger for the check module.

    :return: Logger instance.
    """
    logging.basicConfig(
        filename=f"{wkdir}/parquet_shapefile_check.log",
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s',
        filemode='w'
    )
    logging.getLogger().addHandler(logging.StreamHandler())
    return logging.getLogger()

class ShapefileParquetChecker:

    def __init__(self, shapefile_dir, failed_files=None):
        self.shapefile_dir = shapefile_dir
        self.failed_files = failed_files

    def list_shapefiles(self, file_name_selector=None):
        """
        List all shapefiles that match the provided file name selector.

        :param file_name_selector: List of file name prefixes for filtering.
        :return: List of matching shapefile paths.
        """
        shapefile_dir = self.shapefile_dir
        shapefile_paths = [
            os.path.join(shapefile_dir, file)
            for file in os.listdir(shapefile_dir)
            if file.endswith('.shp') and (file_name_selector is None or any(file.startswith(prefix) for prefix in file_name_selector))
        ]
        return shapefile_paths

    def process_shapefiles(self, shapefile_paths):
        """
        Process the provided shapefiles.

        :param shapefile_paths: List of shapefile paths to process.
        """
        for path in shapefile_paths:
            # Placeholder for actual shapefile processing logic
            check_logger.info(f'adding shapefile to check: {path}')

    def run(self, file_name_selector=None):
        """
        Execute the shapefile processing workflow.

        :param file_name_selector: List of file name prefixes for filtering.
        """
        shapefile_paths = self.list_shapefiles(file_name_selector)
        self.process_shapefiles(shapefile_paths)
        check_logger.info('Shapefile processing completed.')

def read_parquet(parquet_path):
    """Read a parquet file and return a Dask DataFrame."""
    return dd.read_parquet(parquet_path)

def get_unique_gui(dgdf):
    """Get unique values from the 'gui' column."""
    return dgdf['gui'].unique().compute()

def lookup_failed_files(self):
    """Lookup failed files in the log file."""
    failed_files_csv = self.failed_files
    if not os.path.exists(failed_files_csv):
        return []
    return pd.read_csv(failed_files_csv)
def compare_gui_with_shapefiles(shapefile_processor, parquet_path, file_name_selector=None):
    """
    Compare the 'gui' values from the parquet file with the shapefile names.

    :param shapefile_processor: Instance of ShapefileProcessor.
    :param parquet_path: Path to the parquet file.
    :param file_name_selector: List of file name prefixes for filtering.
    """
    dgdf = read_parquet(parquet_path)
    unique_gui = get_unique_gui(dgdf)
    
    shapefile_processor.run(file_name_selector)
    failed_files_df = lookup_failed_files(shapefile_processor)
    # Compare GUIs with shapefiles
    shapefile_paths = shapefile_processor.list_shapefiles(file_name_selector)
    shapefile_names = [os.path.basename(path) for path in shapefile_paths]

    # Reverse check for shapefiles not in GUI
    for shapefile_name in shapefile_names:
        if not any(gui in shapefile_name for gui in unique_gui if not pd.isna(gui)):
            if shapefile_name in failed_files_df['file_name'].values:
                check_logger.info(f'{shapefile_name} not found in the GUI list. Found in failed files.')
            elif shapefile_name not in failed_files_df['file_name'].values:
                check_logger.error(f'{shapefile_name} not found in the GUI list and not in failed files, error with Pipeline')
                print(f'{shapefile_name} not found in the GUI list')

if __name__ == "__main__":
    wkdir = os.path.dirname(os.path.abspath(__file__))
    check_logger = make_check_logger(wkdir)
    shapefile_dir = f'{wkdir}/../src/data/surveymaps'
    PARQUET_PATH = f'{wkdir}/../src/data/parquet/concatenated.parquet'
    failed_files = f'{wkdir}/../src/data/logs/failed_files.csv'
    
    shapefile_processor = ShapefileParquetChecker(shapefile_dir, failed_files)
    compare_gui_with_shapefiles(shapefile_processor, PARQUET_PATH, file_name_selector=None)
