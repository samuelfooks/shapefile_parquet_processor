import os
import logging
import geopandas as gpd
import pandas as pd
import dask.dataframe as dd
from shapely import wkt, wkb, to_wkb
import dask_geopandas as dgpd
import math
from typing import List, Dict
import pyarrow as pa
import dask
import pyarrow.parquet as pq
import json
from dask.distributed import Client, LocalCluster
import json

# Configuration Loader
def load_config(file_path: str):
    """Load the configuration from a JSON file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    with open(file_path, 'r') as f:
        config = json.load(f)
    return config

# Main Function
def main():
    wkdir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(wkdir, "config.json")  # Updated path construction
    try:
        config = load_config(config_path)
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return

    # Access configuration values
    input_dir = config.get("input_dir")
    columns_to_convert = config.get("columns_to_convert")
    output_dir = config.get("output_dir")
    log_dir = config.get("log_dir")
    file_name_selectors = config.get("file_name_selectors")
    
    columns_to_convert = config.get("columns_to_convert")

    # Example usage of configuration values
    print(f"Input Directory: {input_dir}")
    print(f"Output Directory: {output_dir}")
    print(f"Log Directory: {log_dir}")
    print(f"File Name Selectors: {file_name_selectors}")
    print(f"Columns to Convert: {columns_to_convert}")
    # Initialize Dask Client
   
    workflow_manager = WorkflowManager(
        input_dir=os.path.join(wkdir, input_dir),
        output_dir=os.path.join(wkdir, output_dir),
        log_dir=os.path.join(wkdir, log_dir),
        file_name_selectors=file_name_selectors,
        columns_to_convert=columns_to_convert
    )
    workflow_manager.run_workflow()
    # Optionally, close the Dask client
class Logger:
    def __init__(self, log_dir: str, log_file: str = 'shapefile_processing.log'):
        self.log_dir = log_dir
        self.log_file = log_file
        self.setup_logging()

    def setup_logging(self):
        os.makedirs(self.log_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(self.log_dir, self.log_file),
            level=logging.INFO,
            format='%(asctime)s %(levelname)s:%(message)s',
            filemode='w'
        )
        logging.getLogger().addHandler(logging.StreamHandler())
    
    def get_logger(self):
        return logging.getLogger()

def clean_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
        
        return

    for root, dirs, files in os.walk(dir, topdown=False):
        # Remove all files
        for file in files:
            os.remove(os.path.join(root, file))
        
        # Remove all directories
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if not os.listdir(dir_path):  # Check if the directory is empty
                os.rmdir(dir_path)


class ShapefileProcessor:
    def __init__(self, input_dir: str, output_dir: str, logger: logging.Logger, file_name_selectors: List[str] = None, columns_to_convert: List[str] = None):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.logger = logger
        self.file_name_selectors = file_name_selectors if file_name_selectors else []
        self.columns_to_convert = columns_to_convert
        self.failed_files = []

    def make_list(self) -> List[str]:
        shapefiles = []
        for file in os.listdir(self.input_dir):
            if file.endswith('.shp'):
                if not self.file_name_selectors:
                    shapefiles.append(os.path.join(self.input_dir, file))
                    continue
                if any(code in file for code in self.file_name_selectors):
                    shapefiles.append(os.path.join(self.input_dir, file))
        self.logger.info(f'Found {len(shapefiles)} shapefiles to process.')
        return shapefiles

    def clean_geometry(self, gdframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # drop null geometries
        geoms = gdframe['geometry']
        # report null geometries
        null_geoms = gdframe[geoms.isnull()]
        if not null_geoms.empty:
            self.logger.warning(f'Found {len(null_geoms)} rows with null geometries. Dropping these rows')
            gdframe = gdframe[~geoms.isnull()]
        return gdframe

    def select_columns(self, gdframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        for col in self.columns_to_convert:
            if col not in gdframe.columns:
                self.logger.warning(f'Column {col} not found in shapefile. filling with None')
                gdframe[col] = None
        gdframe = gdframe[self.columns_to_convert + ['geometry']]  
        return gdframe

    def align_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ensure consistent data types, downcast to reduce memory usage, use float for NaN and dask compatibility
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
            elif df[col].dtype == 'int64' or df[col].dtype == 'int32':
                df[col] = df[col].astype('float32')
                # check for None values
                if df[col].isnull().any():
                    df[col] = df[col].astype('float32')

            elif df[col].dtype == 'float64':
                df[col] = df[col].astype('float32')
            
            # uniform datetime to datetime64[ms]
            elif 'datetime' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # make into dask dataframe, write to parquet. Append to existing parquet files,
        return df


    def process_shapefile(self, file: str):
        self.logger.info(f'Processing shapefile: {file}')
        try:
            gdf = gpd.read_file(file)
            gdf = gdf.to_crs(epsg=4326)
            # lower case columns
            gdf.rename(columns={col: col.lower() for col in gdf.columns}, inplace=True)
            gdf = self.clean_geometry(gdf)
            # use selected columns if given, include geometry
        
            if self.columns_to_convert:
                gdf = self.select_columns(gdf)
                
            # convert geometry to wkt
            gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt)
            df = pd.DataFrame(gdf)
            
            df = self.align_dtypes(df)
           
            ddf = dd.from_pandas(df, npartitions=1)
            name_function = lambda x: f"{os.path.splitext(os.path.basename(file))[0]}-{x}.parquet"
            ddf.to_parquet(
                self.output_dir,
                engine='pyarrow',
                compression='snappy',
                append=True,
                ignore_divisions=False,
                write_index=False,
                name_function=name_function
            )
            self.logger.info(f'Successfully processed shapefile {file}')
        except Exception as e:
            self.logger.error(f'Failed to process shapefile {file}. Reason: {e}')
            schema_failed_file = {'file_name': os.path.basename(file)}
            try:
                for column in gdf.columns:
                    schema_failed_file[column] = str(gdf[column].dtype)
            except Exception as inner_e:
                self.logger.error(f'Failed to retrieve schema for {file}. Reason: {inner_e}')
            self.failed_files.append(schema_failed_file)

    def process_all_shapefiles(self):
        shapefiles = self.make_list()
        for file in shapefiles:
            self.process_shapefile(file)
        self.failed_files_df = pd.DataFrame(self.failed_files)
        self.logger.info(f'Failed to process {len(self.failed_files)} shapefiles.')
        self.failed_files_df.to_csv(os.path.join(self.output_dir, 'failed_files.csv'), index=False)


class ParquetConcatenator:
    def __init__(self, pq_input_dir: str, concat_dir: str, logger: logging.Logger, chunk_size_mb: int = 200):
        self.pq_input_dir = pq_input_dir
        self.concat_dir = concat_dir
        self.logger = logger
        self.chunk_size_mb = chunk_size_mb

    def get_total_size(self, parquet_paths: List[str]) -> int:
        """Returns the total size in bytes of all parquet files."""
        total_size = 0
        for path in parquet_paths:
            total_size += os.path.getsize(path)
        return total_size

    def concat_parquets_in_chunks(self):
        parquet_files = [f for f in os.listdir(self.pq_input_dir) if f.endswith('.parquet')]
        if not parquet_files:
            self.logger.error('No Parquet files found to concatenate.')
            return
        
        parquet_paths = [os.path.join(self.pq_input_dir, f) for f in parquet_files]
        self.logger.info(f'Found {len(parquet_paths)} Parquet files for concatenation.')

        total_size = self.get_total_size(parquet_paths)
        chunk_size_bytes = self.chunk_size_mb * 1024 * 1024
        num_chunks = math.ceil(total_size / chunk_size_bytes)

        self.logger.info(f'Concatenating into chunks of at least {self.chunk_size_mb} MB 
        Total size of Parquet files: {total_size} bytes.')

        if num_chunks == 0:
            self.logger.error('Total size of Parquet files is zero. Nothing to concatenate.')
            return

        # Group parquet files into chunks
        grouped_files: Dict[int, List[str]] = {}
        current_chunk_size = 0
        current_chunk_index = 0

        for path in parquet_paths:
            file_size = os.path.getsize(path)
            if current_chunk_size + file_size > chunk_size_bytes:
                current_chunk_index += 1
                current_chunk_size = 0
            
            if current_chunk_index not in grouped_files:
                grouped_files[current_chunk_index] = []
            grouped_files[current_chunk_index].append(path)
            current_chunk_size += file_size

        self.logger.info(f'Grouped Parquet files into {len(grouped_files)} chunks.')
        # Process each chunk iteratively
        concatenated_path = os.path.join(self.concat_dir, 'concatenated.parquet')
        
        for chunk_index, chunk_files in grouped_files.items():
            try:
                self.logger.info(f'Processing chunk {chunk_index + 1}/{len(grouped_files)}: {chunk_files}')
                ddf = dd.read_parquet(chunk_files)
                ddf = ddf.repartition(npartitions=1)
                
                ddf.to_parquet(
                    concatenated_path,
                    engine='pyarrow',
                    compression='snappy',
                    append=True,
                    ignore_divisions=False,
                    write_index=False,
                    
                )
                self.logger.info(f'Successfully concatenated chunk {chunk_index + 1} into {self.output_dir}')
            except Exception as e:
                self.logger.error(f'Failed to concatenate chunk {chunk_index + 1} with files {chunk_files}. Error: {e}')

        self.logger.info(f'Concatenation process completed. Total chunks created: {len(grouped_files)}')


class GeoparquetTransformer:
    def __init__(self, concat_dir: str, geopq_dir: str, logger: logging.Logger, chunk_size_mb: int = 500):
        self.concat_dir = concat_dir
        self.geopq_dir = geopq_dir
        os.makedirs(self.geopq_dir, exist_ok=True)
        self.logger = logger
        self.chunk_size_mb = chunk_size_mb

    def process_concatenated_partitions(self):
        concat_parquet = os.path.join(self.concat_dir, 'concatenated.parquet')
        self.logger.info(f'Processing concatenated Parquet file: {concat_parquet}')
        # open the concatenated parquet file
        ddf = dd.read_parquet(concat_parquet)

        # Define a function to convert each partition to a GeoDataFrame
        def convert_partition_to_gdf(partition):
            partition['geometry'] = partition['geometry'].apply(wkt.loads)
            gdf = gpd.GeoDataFrame(partition, geometry='geometry')
            gdf = gdf.set_crs(epsg=4326)
            return gdf
        
        # Define the metadata columns
        meta_cols = ddf.columns.tolist()
        self.logger.info('mapping partitions to GeoDataFrames')
        
        # List to store paths of successfully processed partitions
        successful_partitions = []
        delayed_tasks = []

        # per partition
        for i, partition in enumerate(ddf.to_delayed()):
            def process_partition(partition, i):
                try:
                    gdf = partition.map_partitions(convert_partition_to_gdf, meta_cols = meta_cols)
                    partition_path = os.path.join(self.geopq_dir, f'geoparquet_partition_{i}.parquet')
                    gdf.to_parquet(
                        partition_path,
                        engine='pyarrow',
                        compression='snappy'
                    )
                    self.logger.info(f'Processed partition {i} into GeoParquet format.')
                    return partition_path
                except Exception as e:
                    self.logger.error(f'concatentation geoparquet {i} error {e}')
                    return None

            delayed_task = dask.delayed(process_partition)(partition, i)
            delayed_tasks.append(delayed_task)

        # Trigger computation
        results = dask.compute(*delayed_tasks)

        # Collect successful partition paths
        self.successful_partitions = [res for res in results if res is not None]

    def concatenate_geoparquet_partitions(self):
        successful_partitions = self.successful_partitions
        # Combine all successfully processed GeoParquet partitions into a single GeoDataFrame
        if successful_partitions:
            combined_gdf = dgpd.read_parquet(successful_partitions)
        else:
            self.logger.error('No partitions were successfully processed.')
        
        # Write the final combined GeoDataFrame to a Parquet file
        final_parquet_path = os.path.join(self.geopq_dir, '..', 'concatenated_geoparquet.parquet')

        combined_gdf = combined_gdf.repartition(npartitions=1)

        combined_gdf.geometry = combined_gdf.geometry.apply(lambda geom: wkb.dumps(geom))
        
        combined_gdf.to_parquet(
            final_parquet_path,
            engine='pyarrow',
            compression='snappy',
        )
        
        self.logger.info('Successfully concatenated all shapefiles into a single GeoParquet file.')
        

class WorkflowManager:
    def __init__(self, input_dir: str, output_dir: str, log_dir: str, file_name_selectors: List[str] = None, columns_to_convert: List[str] = None):
        self.logger_instance = Logger(log_dir)
        self.logger = self.logger_instance.get_logger()
        self.input_dir = input_dir
        self.output_dir = output_dir
        clean_dir(self.output_dir)
        self.shp_pq = f"{self.output_dir}/shp_pq"
        self.concat_pq = f"{self.output_dir}/concat_pq"
        self.geopq_dir = f"{self.output_dir}/geoparquet"
        self.file_name_selectors = file_name_selectors if file_name_selectors else []
        self.shapefile_processor = ShapefileProcessor(input_dir,self.shp_pq, self.logger, self.file_name_selectors, columns_to_convert)
        self.parquet_concatenator = ParquetConcatenator(self.shp_pq, self.concat_pq, self.logger)
        self.geoparquet_consolidator = GeoparquetTransformer(self.concat_pq, self.geopq_dir, self.logger) 
        
    def run_workflow(self):
        self.shapefile_processor.process_all_shapefiles()
        self.parquet_concatenator.concat_parquets_in_chunks()
        self.geoparquet_consolidator.process_concatenated_partitions()
        self.geoparquet_consolidator.concatenate_geoparquet_partitions()

if __name__ == '__main__':
    
    with LocalCluster(n_workers=2, threads_per_worker=2) as cluster:
        client = Client(cluster)
        print(f"LocalCluster started")
        main()
        client.close()

    