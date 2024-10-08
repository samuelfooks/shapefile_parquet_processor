import os
import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
import logging
import random

def make_check_logger(wkdir):
    """Create a logger for the check module."""
    logging.basicConfig(
        filename=f"{wkdir}/parquet_shapefile_check.log",
        level=logging.INFO,
        format='%(asctime)s %(message)s',
        filemode='w'
    )
    logging.getLogger().addHandler(logging.StreamHandler())
    return logging.getLogger()

class ShapefileParquetChecker:

    def __init__(self, shapefile_dir, failed_files_path=None):
        self.shapefile_dir = shapefile_dir
        self.failed_files_path = failed_files_path

    def load_failed_files(self):
        """Load the list of failed shapefiles from a CSV."""
        if self.failed_files_path and os.path.exists(self.failed_files_path):
            failed_files_df = pd.read_csv(self.failed_files_path)
            return failed_files_df['file_name'].tolist()  # Adjust the column name as needed
        return []

    def list_shapefiles(self, failed_files):
        """List all shapefiles that are not in the failed files list."""
        shapefile_paths = [
            os.path.join(self.shapefile_dir, file)
            for file in os.listdir(self.shapefile_dir)
            if file.endswith('.shp') and file not in failed_files
        ]
        return shapefile_paths

    def process_shapefiles(self, shapefile_paths):
        """Process the provided shapefiles."""
        for path in shapefile_paths:
            check_logger.info(f'Processing shapefile: {path}')

    def run(self):
        """Execute the shapefile processing workflow."""
        failed_files = self.load_failed_files()
        shapefile_paths = self.list_shapefiles(failed_files)
        self.process_shapefiles(shapefile_paths)
        check_logger.info('Shapefile processing completed.')

def read_parquet(parquet_path):
    """Read a parquet file and return a Dask DataFrame."""
    return dd.read_parquet(parquet_path)

def compare_geometries_with_parquet(shapefile_processor, parquet_path):
    """Compare geometries and gui values of three random shapefiles with the parquet file."""
    # Load the failed files
    failed_files = shapefile_processor.load_failed_files()
    
    # List all shapefiles that are not in the failed files
    shapefile_paths = shapefile_processor.list_shapefiles(failed_files)

    if len(shapefile_paths) < 3:
        check_logger.error('Not enough valid shapefiles found for comparison (need at least 3).')
        return

    # Randomly select three shapefiles
    selected_shapefiles = random.sample(shapefile_paths, 3)
    check_logger.info(f'Selected shapefiles: {selected_shapefiles}')

    # Initialize a list to store all rows from the selected shapefiles
    all_rows = []

    # Extract all rows from the selected shapefiles
    for shapefile in selected_shapefiles:
        gdf = gpd.read_file(shapefile)
        all_rows.append(gdf)

    # Concatenate all rows into a single GeoDataFrame
    all_rows_gdf = pd.concat(all_rows, ignore_index=True)

    # Read the parquet file into a Dask DataFrame
    dgdf = read_parquet(parquet_path)

    columns = 
    # Check for geometries and compare gui values in the parquet file
    for _, row in all_rows_gdf.iterrows():
        wkt_geom = row.geometry.wkt
        gui_value = row['gui']  # Assuming the column is named 'gui'

        # Check if the geometry exists in the parquet file
        matching_rows = dgdf[dgdf['geometry_column_name'].astype(str) == wkt_geom].compute()  # Adjust 'geometry_column_name' as needed
        
        if not matching_rows.empty:
            parquet_gui_values = matching_rows['gui'].tolist()  # Assuming the parquet has a 'gui' column
            
            # Check if the gui values match
            if gui_value in parquet_gui_values:
                check_logger.info(f'Matching geometry found: {wkt_geom} with matching gui value: {gui_value}')
            else:
                check_logger.warning(f'GUI value {gui_value} from shapefile does not match with parquet for geometry: {wkt_geom}')
                print(f'GUI value {gui_value} does not match with parquet for geometry: {wkt_geom}')
        else:
            check_logger.warning(f'No matching rows found in parquet for geometry: {wkt_geom}')

if __name__ == "__main__":
    wkdir = os.path.dirname(os.path.abspath(__file__))
    check_logger = make_check_logger(wkdir)
    
    shapefile_dir = f'{wkdir}/../src/data/surveymaps'
    PARQUET_PATH = f'{wkdir}/../src/data/parquet/concatenated_chunks'
    failed_files = f'{wkdir}/../src/data/logs/failed_files.csv'
    
    shapefile_processor = ShapefileParquetChecker(shapefile_dir, failed_files)
    
    compare_geometries_with_parquet(shapefile_processor, PARQUET_PATH)


