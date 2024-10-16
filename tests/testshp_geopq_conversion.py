import os
import pandas as pd
import geopandas as gpd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import dask_geopandas as dgpd
import dask.dataframe as dd
import contextily as ctx
from shapely import wkt, wkb
from shapely.geometry import shape
import logging
import json
import random as rand

# Configuration Loader
def load_config(file_path: str):
    """Load the configuration from a JSON file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    with open(file_path, 'r') as f:
        config = json.load(f)
    return config

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

def list_failed_files(failed_files_csv):
    """List the failed files from the log file."""
    if not os.path.exists(failed_files_csv):
        check_logger.info('No failed files csv found')
        return []
    return pd.read_csv(failed_files_csv)

def select_shapefiles(shapefile_dir, file_name_selector=None):
    """
    Select shapefiles from the provided directory.

    :param shapefile_dir: Path to the directory containing the shapefiles.
    :param file_name_selector: List of file name prefixes for filtering.
    :return: List of matching shapefile paths.
    """
    shapefile_paths = [
        os.path.join(shapefile_dir, file)
        for file in os.listdir(shapefile_dir)
        if file.endswith('.shp') and (file_name_selector is None or any(file.startswith(prefix) for prefix in file_name_selector))
    ]
    return shapefile_paths

def read_geoparquet(geoparquet_path):
    """Read a parquet file and return a Dask DataFrame."""
    dgdf = dgpd.read_parquet(geoparquet_path)
    dgdf.columns = [col.lower() for col in dgdf.columns]
    print(dgdf.length)
    return dgdf

def read_standard_parquet(parquet_path):
    """Read a parquet file and return a Dask DataFrame."""

    ddf = dd.read_parquet(parquet_path)
    ddf.columns = [col.lower() for col in ddf.columns]
    return ddf

def lookup_gui(dgdf, shapfile_gui):
    """Get unique values from the 'gui' column."""
    check_logger.info(f'Checking for GUI in the Dask DataFrame')

    subset_df = dgdf[dgdf['gui'] == shapfile_gui]
    if subset_df.shape[0] > 0:
        check_logger.info(f'GUI found in the Dask DataFrame')
    return subset_df

def lookup_geoms(dgdf, geom, shape_gui):
    """Get geometries from the Dask DataFrame."""
    from shapely import wkt, wkb
    from shapely.geometry import shape

    if isinstance(geom, str):
        geom = wkt.loads(geom)

    def process_partition(partition, partition_info=None):
        # Log the partition being processed
        partition_id = partition_info['number'] if partition_info else 'unknown'
        check_logger.info(f"Processing partition {partition_id}")
        # Ensure the geometry column contains valid geometry objects, load binary to geometries
        partition['geometry'] = partition['geometry'].apply(
            lambda x: wkb.loads(x) if isinstance(x, bytes) else shape(x) if isinstance(x, str) else x
        )
        # Filter the partition
        subset = partition[partition['geometry'].apply(lambda x: x.contains(geom))]

        # check for exact match
        if subset.shape[0] == 0:
            check_logger.warning('Geometry not found in partition')
        
        subset_geoms = subset['geometry'].values
        for subset_geom in subset_geoms:
            if subset_geom.equals_exact(geom, tolerance=0.001):
                check_logger.info(f'Geometry matches with shapefile geometry in partition {partition_id}')
                return subset[subset['geometry'] == geom]
            else:
                check_logger.error('parquet Geometry contains shape geometry but not exact match')
        return partition.iloc[0:0]
    
    # pass in metadata for dask to read ddf
    meta = dgdf._meta

    # Use map_partitions with partition_info=True to pass partition metadata
    subset = dgdf.map_partitions(
        process_partition, meta=meta, partition_info=True
    ).compute()

    # Handle the result after computing all partitions
    if subset.shape[0] > 0:
        if 'gui' in subset.columns:
            subset_geom_gui = subset[subset['geometry'] == geom]['gui'].values[0]
            if subset_geom_gui == shape_gui:
                check_logger.info(f'GUI matches with shapefile GUI')
                return subset.loc[subset['geometry'] == geom]
            else:
                check_logger.error('GUI mismatch with shapefile GUI')
        else:
            check_logger.error('No GUI column found in the Dask DataFrame')
    else:
        check_logger.error(f'No geometries contain the shapefile geom {geom}')

def compare_with_shapefiles(failed_files_csv, parquet_path, shapefile_dir, config):
    """
    Compare the 'gui' values from the parquet file with the shapefile names.

    :param shapefile_processor: Instance of ShapefileProcessor.
    :param parquet_path: Path to the parquet file.
    :param file_name_selector: List of file name prefixes for filtering.
    """
    #dgdf = read_geoparquet(parquet_path)

    if 'file_name_selectors' in config:
        file_name_selectors = config['file_name_selectors']
    else:
        file_name_selectors = None

    dgdf = read_standard_parquet(parquet_path)
    failed_files_df = list_failed_files(failed_files_csv)
    if len(failed_files_df) == 0:
        logging.info('No failed files found')

    shapefile_paths = select_shapefiles(shapefile_dir, file_name_selectors)

    # get 3 random shapefiles, sample 5 geometries, find in the parquet, if found, check for GUI match
    for i in range(3):
        random_shapefile = shapefile_paths[rand.randint(0, len(shapefile_paths) - 1)]
        
        shapefile_name = os.path.basename(random_shapefile)
        check_logger.info(f'Testing shapefile: {shapefile_name}')

        if len(failed_files_df) > 0 and shapefile_name in failed_files_df['file_name'].values:
            check_logger.info(f'{shapefile_name} found in failed files. Skipping...')
            continue    
        
        shapefile_gdf = gpd.read_file(random_shapefile)
        shapefile_gdf.columns = [col.lower() for col in shapefile_gdf.columns]
        # gui should be all the same for single shapefile
        unique_gui = shapefile_gdf['gui'].unique()
        if len(unique_gui) > 1:
            check_logger.error(f'Multiple GUIs found in the shapefile')
        shape_gui = shapefile_gdf['gui'].values[0]
        
        # get 5 random geometries from the shapefile
        rand_shape_geoms = shapefile_gdf['geometry'].sample(5)
        for shape_geom in rand_shape_geoms:
            check_logger.info(f'Checking geometry: {shape_geom}')
            matched_row = lookup_geoms(dgdf, shape_geom, shape_gui)
            if matched_row is None:
                check_logger.error(f'geometry not found in the Dask DataFrame')
                continue
            if matched_row.shape[0] > 0:
                check_logger.info(f'shape geometry {shape_geom} found in parquet and matches with the shape GUI')
                check_logger.info(f"shape row {shapefile_gdf.loc[shapefile_gdf['geometry'] == shape_geom]} parquet {matched_row}")

    check_logger.info(f'Checks finished')

if __name__ == "__main__":
    wkdir = os.path.dirname(os.path.abspath(__file__))
    check_logger = make_check_logger(wkdir)
    shapefile_dir = f'{wkdir}/../src/data/surveymaps'
    geoparquet_pt_dir = f'{wkdir}/../src/data/parquet/geopq_parts'
    failed_files = f'{wkdir}/../src/data/logs/failed_files.csv'
    configfile = f'{wkdir}/../src/shapefiles_to_parquet/config.json'

    config = load_config(configfile)
    
    compare_with_shapefiles(failed_files, geoparquet_pt_dir, shapefile_dir, config)