import os
import pandas as pd
import geopandas as gpd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import dask_geopandas as dgpd
import dask.dataframe as dd
import contextily as ctx
from shapely import wkt

def read_parquet(parquet_path):
    """
    Read a parquet file and return a Dask DataFrame.

    :param parquet_path: Path to the parquet file.
    :return: Dask DataFrame.
    """
    return dd.read_parquet(parquet_path)

def get_unique_gui(dgdf):
    """
    Get unique values from the 'gui' column.

    :param dgdf: Dask DataFrame.
    :return: Unique values in the 'gui' column.
    """
    return dgdf['gui'].unique().compute()

def filter_rows_with_fr(dgdf):
    """
    Filter rows with 'FR' in the 'gui' column.

    :param dgdf: Dask DataFrame.
    :return: Filtered DataFrame.
    """
    return dgdf[dgdf['gui'].str.contains('FR')].compute()

def plot_geometries(gdf):
    """
    Plot geometries on a map.

    :param gdf: GeoDataFrame with geometries.
    """
    ax = gdf.plot(figsize=(10, 10), alpha=0.5, edgecolor='k')
    ctx.add_basemap(ax, crs=gdf.crs, source=ctx.providers.Stamen.TonerLite)
    plt.show()

def view_test_parquet(parquet_path, shp_dir):
    """
    Main function to execute the parquet view test.

    :param parquet_path: Path to the parquet file.
    :param shp_dir: Directory containing shapefiles.
    """
    dgdf = read_parquet(parquet_path)
    
    unique_gui = get_unique_gui(dgdf)
    print(unique_gui)

    filtered_df = filter_rows_with_fr(dgdf)
    print(filtered_df)

    shpfiles = [file for file in os.listdir(shp_dir) if file.endswith('.shp')]

    for shpfile in shpfiles:
        if shpfile not in unique_gui:
            print(f'{shpfile} not in the unique gui list')
        else:
            print(f'{shpfile} is in the unique gui list')

    ddf = read_parquet(parquet_path)

    for gui in unique_gui:
        if 'FR' in gui:
            df = ddf[ddf['gui'] == gui].compute()
            df['geometry'] = df['geometry'].apply(wkt.loads)
            gdf = gpd.GeoDataFrame(df, geometry='geometry')
            gdf.crs = 'EPSG:4326'
            gdf = gdf.to_crs('EPSG:3857')
            plot_geometries(gdf)
            break

if __name__ == "__main__":
    PARQUET_PATH = '../data/sbh_survey_parquet/combined_data_pyarrow.parquet'
    SHP_DIR = '../data/survey_maps'
    view_test_parquet(PARQUET_PATH, SHP_DIR)
