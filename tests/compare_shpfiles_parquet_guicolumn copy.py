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

    def __init__(self, shapefile_dir, failed_files, parquet_path):
        self.shapefile_dir = shapefile_dir
        self.failed_files = failed_files
        self.parquet_path = parquet_path
        

    def select_shapefiles(self, file_name_selector=None):
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
        self.shapefiles_to_check = shapefile_paths

    def read_parquet(self, parquet_path):
        """Read a parquet file and return a Dask DataFrame."""
        return dd.read_parquet(parquet_path)

    def get_unique_gui(self, dgdf):
        """Get unique values from the 'gui' column."""
        return dgdf['gui'].unique().compute()

    def lookup_failed_files(self):
        """Lookup failed files in the log file."""
        failed_files_csv = self.failed_files
        if not os.path.exists(failed_files_csv):
            return []
        return pd.read_csv(failed_files_csv)

    def compare_gui_with_shapefiles(self):
        """
        Compare the 'gui' values from the parquet file with the shapefile names.

        :param shapefile_processor: Instance of ShapefileProcessor.
        :param parquet_path: Path to the parquet file.
        :param file_name_selector: List of file name prefixes for filtering.
        """
        dgdf = self.read_parquet(self.parquet_path)
        unique_gui = self.get_unique_gui(dgdf)
        
        failed_files_df = self.lookup_failed_files()
        # Compare GUIs with shapefiles
        
        
        # Reverse check for shapefiles not in GUI
        for shapefile_name in self.shapefiles_to_check:
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
    parquet_path = f'{wkdir}/../src/data/parquet/concatenated.parquet'
    failed_files = f'{wkdir}/../src/data/logs/failed_files.csv'
    
    file_name_selector = None
    checker = ShapefileParquetChecker(shapefile_dir, failed_files, parquet_path)
    checker.select_shapefiles(file_name_selector)
    checker.compare_gui_with_shapefiles()
