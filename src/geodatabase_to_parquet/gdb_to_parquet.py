import os
import requests
import zipfile
import fiona
import logging
import dask
from dask.distributed import LocalCluster, Client, progress
import geopandas as gpd
import pandas as pd
import dask_geopandas as dgpd


logger = logging.getLogger(__name__)

def download_and_extract_zip(zip_url, extract_to, geodatabase_name):
    """
    Downloads and extracts a zip file from a given URL and searches for the specified geodatabase.

    Parameters:
    - zip_url (str): URL of the zip file to download.
    - extract_to (str): Directory where the zip file will be extracted.
    - geodatabase_name (str): The name of the geodatabase file to find.

    Returns:
    - str: Path to the extracted geodatabase file.
    """
    try:
        # Download the zip file
        logger.info(f"Downloading zip file from {zip_url}")
        zip_file_path = os.path.join(extract_to, 'downloaded_data.zip')
        response = requests.get(zip_url)
        with open(zip_file_path, 'wb') as file:
            file.write(response.content)
        logger.info(f"Zip file downloaded to {zip_file_path}")

        # Extract the zip file
        logger.info(f"Extracting zip file to {extract_to}")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

        # Find the specified geodatabase (.gdb) file in the extracted files
        for root, dirs, files in os.walk(extract_to):
            for dir_name in dirs:
                if dir_name == geodatabase_name:
                    gdb_path = os.path.join(root, dir_name)
                    logger.info(f"Geodatabase found: {gdb_path}")
                    return gdb_path

        logger.error(f"Geodatabase {geodatabase_name} not found in the extracted files.")
        return None
    except Exception as e:
        logger.error(f"Error downloading or extracting the zip file: {e}")
        return None

def optimize_gdf_types(gdf):
    """
    Optimize data types to reduce memory usage.
    """
    for col in gdf.select_dtypes(include=['object']).columns:
        gdf[col] = gdf[col].astype('category')
    return gdf

def convert_chunk_to_parquet(gdf, chunk_parquet_path, compression='snappy'):
    """
    Converts a GeoDataFrame chunk to a Parquet file.

    Parameters:
    - gdf (GeoDataFrame): The GeoDataFrame chunk to be converted.
    - chunk_parquet_path (str): The file path to save the chunk Parquet file.
    - compression (str): Compression method for the Parquet file.

    Returns:
    - bool: True if conversion is successful, False otherwise.
    """
    try:
        # Convert to Parquet with geometry as WKB
        gdf = gdf.copy()
        gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkb if geom else None)
        gdf.to_parquet(chunk_parquet_path, compression=compression, engine='pyarrow')
        return True
    except Exception as e:
        logger.error(f"Error converting chunk to Parquet: {e}")
        return False

def combine_chunks_and_finalize(chunk_dir, title, compression='snappy'):
    """
    Combines all chunk Parquet files into a final Parquet file.

    Parameters:
    - chunk_dir (str): Directory containing chunk Parquet files.
    - title (str): Title for the final Parquet file.
    - compression (str): Compression method for the final Parquet file.

    Returns:
    - str or None: Path to the final Parquet file or None if failed.
    """
    try:
        # List all chunk Parquet files
        chunk_files = [os.path.join(chunk_dir, f) for f in os.listdir(chunk_dir) if f.endswith('.parquet')]
        if not chunk_files:
            logger.error("No chunk Parquet files found to combine.")
            return None

        logger.info(f"Combining {len(chunk_files)} chunk files.")

        # Read and concatenate all chunks
        df_list = []
        for file in chunk_files:
            df = pd.read_parquet(file)
            df_list.append(df)
        combined_df = pd.concat(df_list, ignore_index=True)

        logger.info("All chunks combined into a single DataFrame.")

        # Define final Parquet file path
        final_parquet_name = f"{title}.parquet"
        final_parquet_path = os.path.join(chunk_dir, final_parquet_name)

        # Save the final Parquet file
        combined_df.to_parquet(final_parquet_path, compression=compression, engine='pyarrow')

        logger.info(f"Final Parquet file saved at {final_parquet_path}")

        return final_parquet_path

    except Exception as e:
        logger.error(f"Error combining chunks: {e}")
        return None

def geodatabase_to_parquet(geodatabase_path, layer_name, output_dir):
    """
    Convert a geodatabase layer to Parquet format using Dask GeoPandas for parallel processing.

    Parameters:
    - geodatabase_path (str): Path to the geodatabase file.
    - layer_name (str): Name of the layer to convert.
    - output_dir (str): Directory where the final Parquet files will be saved.
    """
    try:
        logger.info(f"Processing layer: {layer_name}")

        # Open the GDB layer with Fiona
        with fiona.open(geodatabase_path, layer=layer_name) as src:
            total_features = len(src)
            logger.info(f"{layer_name} has {total_features} features")
            
            # Variables for chunking
            chunk_size = 100000
            chunk = []
            chunk_index = 0
            chunk_dir = os.path.join(output_dir, 'chunks')
            os.makedirs(chunk_dir, exist_ok=True)

            for idx, feature in enumerate(src):
                chunk.append(feature)

                # Process the chunk when it reaches the chunk size
                if len(chunk) >= chunk_size or idx == total_features - 1:
                    logger.info(f"Processing chunk {chunk_index + 1} with {len(chunk)} features")
                    
                    # Convert to GeoDataFrame and process the chunk
                    gdf_chunk = gpd.GeoDataFrame.from_features(chunk, crs=src.crs)
                    gdf_chunk = optimize_gdf_types(gdf_chunk)

                    # Generate a unique Parquet file path for the chunk
                    chunk_parquet_name = f"{layer_name}_chunk_{chunk_index + 1}.parquet"
                    chunk_parquet_path = os.path.join(chunk_dir, chunk_parquet_name)

                    # Convert the chunk to Parquet
                    convert_chunk_to_parquet(gdf_chunk, chunk_parquet_path)

                    # Clear the chunk for the next iteration
                    chunk = []
                    chunk_index += 1
            
            # Combine the processed chunks into the final Parquet file
            title = os.path.splitext(os.path.basename(geodatabase_path))[0]
            final_parquet_path = combine_chunks_and_finalize(chunk_dir, title)
            if final_parquet_path:
                logger.info(f"Final Parquet file created at {final_parquet_path}")
                return final_parquet_path
            else:
                logger.error(f"Failed to create the final Parquet file for layer {layer_name}")
                return None

    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        return None

def gdb_to_parquet():
    """
    Converts Geodatabase (GDB) layers to Parquet format.

    Downloads a zip file, extracts the GDB, checks for necessary variables,
    converts each layer to GeoParquet, and uploads to S3.
    """
    wkdir = os.path.abspath(os.getcwd())
    #zip_url = 'https://s3.waw3-1.cloudferro.com/emodnet/emodnet_native/emodnet_human_activities/energy/wind_farms_points/EMODnet_HA_Energy_WindFarms_20240508.zip'
    #zip_url = 'https://s3.waw3-1.cloudferro.com/emodnet/emodnet_native/emodnet_geology/seabed_substrate/multiscale_folk_5/EMODnet_GEO_Seabed_Substrate_All_Res.zip'
    zip_url = 'https://s3.waw3-1.cloudferro.com/emodnet/emodnet_seabed_habitats/12549/EUSeaMap_2023.zip'
    extract_to = os.path.join(wkdir, 'data')
    # geodatabase_name = 'EMODnet_HA_Energy_WindFarms_20240508.gdb'  # Specify the desired GDB name
    # geodatabase_name = 'EMODnet_Seabed_Substrate_1M.gdb'
    geodatabase_name = 'EUSeaMap_2023.gdb'
    os.makedirs(extract_to, exist_ok=True)
    output_dir = os.path.join(wkdir, 'output_parquet')
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Download and extract the zip file, looking for the specific GDB
    gdb_path = download_and_extract_zip(zip_url, extract_to, geodatabase_name)
    if not gdb_path:
        logger.error("Failed to download and extract the zip file.")
        return

    # Step 2: Identify layers in the extracted GDB file
    gdblayers = fiona.listlayers(gdb_path)
    print(gdblayers)
    index = input('choose a layer by the index')

    gdblayer = gdblayers[int(index)]
   
    logger.info(f"Creating parquet for {gdblayer}")
    
    # Convert the layer to Parquet
    combined_parquet_path = geodatabase_to_parquet(gdb_path, gdblayer, output_dir)

    if combined_parquet_path is not None:
        logger.info(f'{combined_parquet_path} saved successfully')

    else:
        logger.error(f"Error converting {gdblayer} to parquet")

    return


def main():
    gdb_to_parquet()

if __name__ == '__main__':
    worker_threads = 4
    
    with LocalCluster(n_workers=worker_threads, threads_per_worker=1, memory_limit='4GiB') as cluster:
        client = Client(cluster)
        print(client.dashboard_link)
        logger.info(f"LocalCluster started with {worker_threads} workers")
        main()
        client.close()
