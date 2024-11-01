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


def load_config(file_path: str):
    """
    Load the configuration from a JSON file.

    :param file_path: Path to the JSON configuration file.
    :type file_path: str
    :raises FileNotFoundError: If the configuration file does not exist.
    :return: Configuration dictionary.
    :rtype: dict
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    with open(file_path, 'r') as f:
        config = json.load(f)
    return config


def main():
    """
    Main function to execute the workflow.

    This function sets up the working directory, loads the configuration,
    initializes the workflow manager, and runs the workflow.
    """
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
    """
    Logger class to handle logging configurations.

    :param log_dir: Directory where log files will be stored.
    :type log_dir: str
    :param log_file: Name of the log file. Defaults to 'shapefile_processing.log'.
    :type log_file: str, optional
    """
    def __init__(self, log_dir: str, log_file: str = 'shapefile_processing.log'):
        self.log_dir = log_dir
        self.log_file = log_file
        self.setup_logging()

    def setup_logging(self):
        """
        Set up logging configuration.

        Creates the log directory if it doesn't exist and configures the logging settings.
        """
        os.makedirs(self.log_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(self.log_dir, self.log_file),
            level=logging.INFO,
            format='%(asctime)s %(levelname)s:%(message)s',
            filemode='w'
        )
        logging.getLogger().addHandler(logging.StreamHandler())
    
    def get_logger(self):
        """
        Retrieve the configured logger.

        :return: Configured logger instance.
        :rtype: logging.Logger
        """
        return logging.getLogger()


def clean_dir(dir_path: str):
    """
    Clean the specified directory by removing all files and empty subdirectories.

    If the directory does not exist, it is created.

    :param dir_path: Path to the directory to clean.
    :type dir_path: str
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        return

    for root, dirs, files in os.walk(dir_path, topdown=False):
        # Remove all files
        for file in files:
            os.remove(os.path.join(root, file))
        
        # Remove all directories
        for dir in dirs:
            dir_path_full = os.path.join(root, dir)
            if not os.listdir(dir_path_full):  # Check if the directory is empty
                os.rmdir(dir_path_full)


class ShapefileProcessor:
    """
    Processor for handling shapefiles.

    :param input_dir: Directory containing input shapefiles.
    :type input_dir: str
    :param output_dir: Directory where processed Parquet files will be stored.
    :type output_dir: str
    :param logger: Logger instance for logging.
    :type logger: logging.Logger
    :param file_name_selectors: List of selectors to filter shapefiles by name.
    :type file_name_selectors: List[str], optional
    :param columns_to_convert: List of columns to convert in the shapefiles.
    :type columns_to_convert: List[str], optional
    """
    def __init__(self, input_dir: str, output_dir: str, logger: logging.Logger, file_name_selectors: List[str] = None, columns_to_convert: List[str] = None):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.logger = logger
        self.file_name_selectors = file_name_selectors if file_name_selectors else []
        self.columns_to_convert = columns_to_convert
        self.failed_files = []

    def make_list(self) -> List[str]:
        """
        Create a list of shapefile paths to process.

        Filters shapefiles based on the provided file name selectors.

        :return: List of shapefile paths.
        :rtype: List[str]
        """
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
        """
        Clean the geometry column by removing null geometries.

        :param gdframe: GeoDataFrame to clean.
        :type gdframe: gpd.GeoDataFrame
        :return: Cleaned GeoDataFrame without null geometries.
        :rtype: gpd.GeoDataFrame
        """
        # drop null geometries
        geoms = gdframe['geometry']
        # report null geometries
        null_geoms = gdframe[geoms.isnull()]
        if not null_geoms.empty:
            self.logger.warning(f'Found {len(null_geoms)} rows with null geometries. Dropping these rows')
            gdframe = gdframe[~geoms.isnull()]
        return gdframe

    def select_columns(self, gdframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Select and prepare specified columns for conversion.

        Adds missing columns with None values if they do not exist in the GeoDataFrame.

        :param gdframe: GeoDataFrame to select columns from.
        :type gdframe: gpd.GeoDataFrame
        :return: GeoDataFrame with selected columns and geometry.
        :rtype: gpd.GeoDataFrame
        """
        for col in self.columns_to_convert:
            if col not in gdframe.columns:
                self.logger.warning(f'Column {col} not found in shapefile. Filling with None')
                gdframe[col] = None
        gdframe = gdframe[self.columns_to_convert + ['geometry']]  
        return gdframe

    def align_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Align data types of the DataFrame columns for consistency and efficiency.

        Downcasts numerical types to reduce memory usage and ensures compatibility with Dask.

        :param df: DataFrame to align data types.
        :type df: pd.DataFrame
        :return: DataFrame with aligned data types.
        :rtype: pd.DataFrame
        """
        # Ensure consistent data types, downcast to reduce memory usage, use float for NaN and dask compatibility
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
            elif df[col].dtype in ['int64', 'int32']:
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
        """
        Process a single shapefile and convert it to Parquet format.

        :param file: Path to the shapefile to process.
        :type file: str
        """
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
        """
        Process all shapefiles found in the input directory.

        Iterates through the list of shapefiles and processes each one.
        Records any failed file processing attempts.
        """
        shapefiles = self.make_list()
        for file in shapefiles:
            self.process_shapefile(file)
        self.failed_files_df = pd.DataFrame(self.failed_files)
        self.logger.info(f'Failed to process {len(self.failed_files)} shapefiles.')
        self.failed_files_df.to_csv(os.path.join(self.output_dir, 'failed_files.csv'), index=False)


class ParquetConcatenator:
    """
    Concatenates multiple Parquet files into larger chunks.

    :param pq_input_dir: Directory containing input Parquet files.
    :type pq_input_dir: str
    :param concat_dir: Directory where concatenated Parquet files will be stored.
    :type concat_dir: str
    :param logger: Logger instance for logging.
    :type logger: logging.Logger
    :param chunk_size_mb: Maximum size of each concatenated chunk in megabytes. Defaults to 400.
    :type chunk_size_mb: int, optional
    """
    def __init__(self, pq_input_dir: str, concat_dir: str, logger: logging.Logger, chunk_size_mb: int = 400):
        self.pq_input_dir = pq_input_dir
        self.concat_dir = concat_dir
        self.logger = logger
        self.chunk_size_mb = chunk_size_mb
        self.failed_files = []

    def get_total_size(self, parquet_paths: List[str]) -> int:
        """
        Calculate the total size of the given Parquet files.

        :param parquet_paths: List of Parquet file paths.
        :type parquet_paths: List[str]
        :return: Total size in bytes.
        :rtype: int
        """
        total_size = 0
        for path in parquet_paths:
            total_size += os.path.getsize(path)
        return total_size

    def concat_parquets_in_chunks(self):
        """
        Concatenate Parquet files into larger chunks based on the specified chunk size.

        Processes each Parquet file, concatenates them, and writes the concatenated chunks
        to the designated directory. Handles any failures during the concatenation process.
        """
        parquet_files = [f for f in os.listdir(self.pq_input_dir) if f.endswith('.parquet')]
        if not parquet_files:
            self.logger.error('No Parquet files found to concatenate.')
            return
        
        parquet_paths = [os.path.join(self.pq_input_dir, f) for f in parquet_files]
        self.logger.info(f'Found {len(parquet_paths)} Parquet files for concatenation.')

        chunk_size_bytes = self.chunk_size_mb * 1024 * 1024
        concatenated_path = os.path.join(self.concat_dir, 'concatenated.parquet')

        current_ddf = None
        current_size = 0
        chunk_index = 0

        for path in parquet_paths:
            try:
                file_ddf = dd.read_parquet(path)
                file_size = os.path.getsize(path)

                if current_ddf is None:
                    current_ddf = file_ddf
                else:
                    try:
                        current_ddf = dd.concat([current_ddf, file_ddf])
                    except Exception as e:
                        self.logger.error(f'Failed to concatenate file {path}. Error: {e}')
                        self.failed_files.append(path)
                        continue
                current_size += file_size

                if current_size >= chunk_size_bytes:
                    current_ddf = current_ddf.repartition(npartitions=1)
                    self.logger.info(f'Writing chunk {chunk_index + 1} to {concatenated_path}')
                    current_ddf.to_parquet(
                        concatenated_path,
                        engine='pyarrow',
                        compression='snappy',
                        append=True,
                        ignore_divisions=False,
                        write_index=False,
                    )
                    current_ddf = None
                    current_size = 0
                    chunk_index += 1

            except Exception as e:
                self.logger.error(f'Failed to process file {path}. Error: {e}')

        if current_ddf is not None:
            self.logger.info(f'Writing final chunk {chunk_index + 1} to {concatenated_path}')
            current_ddf = current_ddf.repartition(npartitions=1)
            current_ddf.to_parquet(
                concatenated_path,
                engine='pyarrow',
                compression='snappy',
                append=True,
                ignore_divisions=False,
                write_index=False,
            )

        self.logger.info(f'Concatenation process completed. Total chunks created: {chunk_index + 1}')
        if self.failed_files:
            self.logger.error(f'Failed to concatenate {len(self.failed_files)} files. See log for details.')
            with open(os.path.join(self.concat_dir, 'failed_files.txt'), 'w') as f:
                for file in self.failed_files:
                    f.write(file + '\n')
            self.logger.error(f'List of failed files written to {self.concat_dir}/failed_files.txt')


class GeoparquetTransformer:
    """
    Transformer for converting concatenated Parquet files to GeoParquet format.

    :param concat_dir: Directory containing concatenated Parquet files.
    :type concat_dir: str
    :param geopq_dir: Directory where GeoParquet files will be stored.
    :type geopq_dir: str
    :param logger: Logger instance for logging.
    :type logger: logging.Logger
    :param chunk_size_mb: Chunk size in megabytes for processing partitions. Defaults to 500.
    :type chunk_size_mb: int, optional
    """
    def __init__(self, concat_dir: str, geopq_dir: str, logger: logging.Logger, chunk_size_mb: int = 500):
        self.concat_dir = concat_dir
        self.geopq_dir = geopq_dir
        os.makedirs(self.geopq_dir, exist_ok=True)
        self.logger = logger
        self.chunk_size_mb = chunk_size_mb

    def process_concatenated_partitions(self):
        """
        Process concatenated Parquet file partitions and convert them to GeoParquet format.

        Converts the geometry from WKT to Shapely geometries, creates GeoDataFrames,
        and saves each partition as a GeoParquet file.
        """
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
        
        # Map the conversion function across all partitions using Dask's map_partitions
        self.logger.info('Mapping partitions to GeoDataFrames...')
        meta = gpd.GeoDataFrame(columns=ddf.columns, geometry='geometry').set_crs(epsg=4326)
        gdf_ddf = ddf.map_partitions(convert_partition_to_gdf, meta=meta)

        # Save each partition as a GeoParquet file
        def save_partition(partition, i):
            """
            Save a single GeoDataFrame partition to a GeoParquet file.

            :param partition: GeoDataFrame partition to save.
            :type partition: gpd.GeoDataFrame
            :param i: Partition index.
            :type i: int
            :return: Path to the saved GeoParquet file or None if failed.
            :rtype: str or None
            """
            try:
                partition_path = os.path.join(self.geopq_dir, f'geoparquet_partition_{i}.parquet')
                partition.to_parquet(partition_path, engine='pyarrow', compression='snappy')
                self.logger.info(f'Processed partition {i} into GeoParquet format.')
                return partition_path
            except Exception as e:
                self.logger.error(f'Error processing partition {i}: {e}')
                return None

        # Create delayed tasks for saving partitions
        delayed_tasks = [
            dask.delayed(save_partition)(partition, i) 
            for i, partition in enumerate(gdf_ddf.to_delayed())
        ]

        # Execute all tasks
        results = dask.compute(*delayed_tasks)

        # Collect paths of successfully processed partitions
        self.successful_partitions = [res for res in results if res is not None]
        self.logger.info(f'Successfully processed {len(self.successful_partitions)} partitions.')
        
    def concatenate_geoparquet_partitions(self):
        """
        Concatenate all successfully processed GeoParquet partitions into a single GeoParquet file.

        Combines all partitions into a single GeoDataFrame and saves it as a consolidated GeoParquet file.
        """
        successful_partitions = self.successful_partitions
        # Combine all successfully processed GeoParquet partitions into a single GeoDataFrame
        if successful_partitions:
            combined_gdf = dgpd.read_parquet(successful_partitions)
        else:
            self.logger.error('No partitions were successfully processed.')
            return
        
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
    """
    Manager for orchestrating the entire workflow of processing shapefiles to GeoParquet.

    :param input_dir: Directory containing input shapefiles.
    :type input_dir: str
    :param output_dir: Directory where output files will be stored.
    :type output_dir: str
    :param log_dir: Directory where log files will be stored.
    :type log_dir: str
    :param file_name_selectors: List of selectors to filter shapefiles by name.
    :type file_name_selectors: List[str], optional
    :param columns_to_convert: List of columns to convert in the shapefiles.
    :type columns_to_convert: List[str], optional
    """
    def __init__(self, input_dir: str, output_dir: str, log_dir: str, file_name_selectors: List[str] = None, columns_to_convert: List[str] = None):
        self.logger_instance = Logger(log_dir)
        self.logger = self.logger_instance.get_logger()
        self.input_dir = input_dir
        self.output_dir = output_dir
        clean_dir(self.output_dir)
        self.shp_pq = f"{self.output_dir}/shp_pq"
        self.concat_pq = f"{self.output_dir}/concat_pq"
        self.geopq_dir = f"{self.output_dir}/geopq_parts"
        self.file_name_selectors = file_name_selectors if file_name_selectors else []
        self.shapefile_processor = ShapefileProcessor(input_dir, self.shp_pq, self.logger, self.file_name_selectors, columns_to_convert)
        self.parquet_concatenator = ParquetConcatenator(self.shp_pq, self.concat_pq, self.logger)
        self.geoparquet_consolidator = GeoparquetTransformer(self.concat_pq, self.geopq_dir, self.logger) 
        
    def run_workflow(self):
        """
        Execute the entire workflow: processing shapefiles, concatenating Parquet files,
        and transforming them into GeoParquet format.
        """
        self.shapefile_processor.process_all_shapefiles()
        self.parquet_concatenator.concat_parquets_in_chunks()
        self.geoparquet_consolidator.process_concatenated_partitions()
        self.geoparquet_consolidator.concatenate_geoparquet_partitions()


if __name__ == '__main__':
    """
    Entry point for the script.

    Initializes a local Dask cluster and executes the main workflow.
    """
    with LocalCluster(n_workers=2, threads_per_worker=2) as cluster:
        client = Client(cluster)
        print(f"LocalCluster started")
        main()
        client.close()
