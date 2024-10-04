import dask_geopandas as dgpd
import geopandas as gpd
import os
import fiona
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import logging
from shapely import wkb

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_parquet_from_s3(parquet_url):
    """
    Reads a Parquet file from the S3 URL using Dask GeoPandas.

    Parameters:
    - parquet_url (str): S3 URL to the Parquet file.

    Returns:
    - Dask GeoDataFrame: The Dask GeoDataFrame loaded from Parquet.
    """
    logger.info(f"Reading Parquet file from {parquet_url}")
    ddf = dd.read_parquet(parquet_url, columns=['geometry', 'Biozone', 'Energy']) 
    dgdf = dgpd.from_dask_dataframe(ddf)
    return dgdf

def read_gdb_layer(gdb_path, layer_name, start, end):
    """
    Reads features from a specific layer in a Geodatabase (.gdb) file using Fiona.

    Parameters:
    - gdb_path (str): Path to the Geodatabase.
    - layer_name (str): The specific layer name to read from the GDB.
    - start (int): The starting index of features to read.
    - end (int): The ending index of features to read.

    Returns:
    - Dask GeoDataFrame: Dask GeoDataFrame with the specified features from the GDB layer.
    """
    logger.info(f"Reading features from {start} to {end} of GDB layer {layer_name} from {gdb_path}")
    with fiona.open(gdb_path, layer=layer_name) as src:
        features = [f for _, f in zip(range(start, end), src)]
        gdf = gpd.GeoDataFrame.from_features(features, crs=src.crs)
    return dgpd.from_geopandas(gdf, npartitions=4)

def compare_rows(gdf_gdb, ddf_parquet, geometry_column='geometry'):
    """
    Compares geometries between the GDB layer and the Parquet file.

    Parameters:
    - gdf_gdb (GeoDataFrame): GeoDataFrame from the GDB layer.
    - ddf_parquet (Dask GeoDataFrame): Dask GeoDataFrame from the Parquet file.
    - geometry_column (str): The column name containing geometries for comparison.

    Returns:
    - GeoDataFrame: Rows in the GDB layer with geometries that also exist in the Parquet file.
    """
    logger.info(f"Comparing geometries between GDB and Parquet using column: {geometry_column}")

    gdf_gdb_len = gdf_gdb.shape[0].compute()
    
    # Use only the first 1000 rows from the Parquet data
    ddf_parquet_subset = ddf_parquet.head(1000)

    # Directly use WKB geometries from Dask GeoDataFrame
    ddf_parquet_subset['wkb'] = ddf_parquet_subset['geometry']

    # Convert WKB back to geometry objects
    gdf_gdb['testgeometry'] = gdf_gdb['geometry']
    ddf_parquet_subset['testgeometry'] = ddf_parquet_subset['wkb'].apply(wkb.loads)

    # Set active geometry columns
    gdf_gdb = gdf_gdb.set_geometry('testgeometry')
    ddf_parquet_subset = ddf_parquet_subset.set_geometry('testgeometry')

    # Check for duplicates
    gdf_gdb_unique = gdf_gdb.drop_duplicates(subset=['testgeometry'])
    ddf_parquet_unique = ddf_parquet_subset.drop_duplicates(subset=['testgeometry'])

    # Log unique counts
    logger.info(f"Unique geometries in GDB: {gdf_gdb_unique.shape[0].compute()}")
    # logger.info(f"Unique geometries in Parquet: {ddf_parquet_unique.shape[0].compute()}")
    logger.info(f"Unique geometries in Parquet: {ddf_parquet_unique.shape[0]}")

    # Use Dask merge to find matching geometries based on geometry objects
    merged = gdf_gdb.merge(ddf_parquet_unique[['testgeometry']], on='testgeometry', how='inner')

    # Debugging output to check mismatched geometries
    unmatched_gdb = gdf_gdb[~gdf_gdb['testgeometry'].isin(ddf_parquet_unique['testgeometry'])]
    unmatched_parquet = ddf_parquet_unique[~ddf_parquet_unique['testgeometry'].isin(gdf_gdb['testgeometry'])]

    logger.info(f"Unmatched geometries in GDB: {unmatched_gdb.shape[0].compute()}")
    logger.info(f"Unmatched geometries in Parquet: {unmatched_parquet.shape[0]}")

    len_merged = merged.shape[0].compute()
    print(f"len gdb geom {gdf_gdb_len} len merged gdf with pq {len_merged}")
    return len_merged


def main():
    # S3 URL of the Parquet file
    parquet_url = 'output_parquet/chunks_euseamapcomplete/EUSeaMap_2023.parquet'
    # parquet_url = 'https://s3.waw3-1.cloudferro.com/emodnet/emodnet_seabed_habitats/12549/EUSeaMap_2023.parquet'
    #parquet_url = 'output_parquet/chunks/EMODnet_HA_Energy_WindFarms_20240508.parquet'

    #parquet_url = 'output_parquet/chunks/EMODnet_Seabed_Substrate_1M.parquet'
    # Path to the Geodatabase and layer 0 by default

    gdb_path = 'data/EUSeaMap_2023.gdb'
    #gdb_path = 'data/EMODnet_HA_Energy_WindFarms_20240508.gdb'  
    #gdb_path = 'data/EMODnet_GEO_Seabed_Substrate_All_Res/EMODnet_Seabed_Substrate_1M.gdb'

    layer_index = 0
    layer_name = fiona.listlayers(gdb_path)[layer_index]

    # Start Dask LocalCluster
    with LocalCluster(n_workers=2, threads_per_worker=1, memory_limit='8GB') as cluster:
        client = Client(cluster)
        logger.info(f"Cluster started with {client}")

        dgdf_parquet = read_parquet_from_s3(parquet_url)

        # Step 2: Read the first 1000 features from the Parquet layer
        ddf_parquet_subset = dgdf_parquet.head(1000)

        # Step 3: Sequentially read the GDB in chunks of 10,000 features and compare
        gdb_total_features = len(fiona.open(gdb_path, layer=layer_name))  # Get the total number of features in the GDB
        chunk_size = 100000
        
        for start in range(0, gdb_total_features, chunk_size):
            end = min(start + chunk_size, gdb_total_features)
            dgdf_gdb_chunk = read_gdb_layer(gdb_path, layer_name, start, end)

            # Step 4: Compare rows and get matching geometries for the current chunk
            matching_rows = compare_rows(dgdf_gdb_chunk, ddf_parquet_subset)

            # Output result
            if matching_rows > 0:
                logger.info(f"Found {matching_rows} matching rows in chunk {start} to {end}:\n{matching_rows} from parquet in gdb")
            else:
                logger.info(f"No matching rows found in chunk {start} to {end}.")

        client.close()

if __name__ == '__main__':
    main()