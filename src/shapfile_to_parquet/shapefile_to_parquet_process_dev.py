

import os
import logging

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

import geopandas as gpd
import pandas as pd
import dask.dataframe as dd
from typing import List

class ShapefileProcessor:
    def __init__(self, input_dir: str, output_dir: str, logger: logging.Logger, file_name_selectors: List[str] = None):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.logger = logger
        self.file_name_selectors = file_name_selectors if file_name_selectors else []
        self.failed_files = []
        self.clean_dir()

    def clean_dir(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            self.logger.info(f'Created directory: {self.output_dir}')
            return
        for root, dirs, files in os.walk(self.output_dir):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        self.logger.info(f'Cleaned output directory: {self.output_dir}')

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

    def process_shapefile(self, file: str):
        self.logger.info(f'Processing shapefile: {file}')
        try:
            gdf = gpd.read_file(file)
            gdf = gdf.to_crs(epsg=4326)
            gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom else None)
            
            df = pd.DataFrame(gdf)
            
            # Ensure consistent data types
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str)
                elif df[col].dtype == 'int64' or df[col].dtype == 'int32':
                    df[col] = df[col].astype('float32')
                elif df[col].dtype == 'float64':
                    df[col] = df[col].astype('float32')
            
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
        return self.failed_files

import pyarrow.parquet as pq

class FailedFileCorrector:
    def __init__(self, input_dir: str, output_dir: str, logger: logging.Logger, failed_files: List[dict]):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.logger = logger
        self.failed_files = failed_files

    def correct_failed_file(self, failed_file: dict):
        self.logger.info(f'Correcting failed file: {failed_file["file_name"]}')
        parquet_files = [f for f in os.listdir(self.output_dir) if f.endswith('.parquet')]
        if not parquet_files:
            self.logger.error('No parquet files found in the output directory.')
            return
        last_parquet = parquet_files[-1]
        try:
            parquet = pq.read_table(os.path.join(self.output_dir, last_parquet))
            majority_schema = parquet.schema
            majority_columns = [field.name for field in majority_schema]
            self.logger.info(f'Majority schema columns: {majority_columns}')

            gdf = gpd.read_file(os.path.join(self.input_dir, failed_file['file_name']))
            gdf = gdf.to_crs(epsg=4326)
            gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom else None)
            df = pd.DataFrame(gdf)
            
            dtype_mismatches = {}
            for col in gdf.columns:
                if col not in majority_columns:
                    self.logger.error(f'Column {col} not found in the majority schema. Cannot append this file.')
                    return
                parquet_dtype = str(majority_schema.field(col).type)
                df_dtype = str(df[col].dtype)
                if df_dtype != parquet_dtype:
                    self.logger.warning(f"Column '{col}' has dtype mismatch: {df_dtype} (shapefile) vs {parquet_dtype} (Parquet).")
                    dtype_mismatches[col] = {"parquet_dtype": parquet_dtype, "shapefile_dtype": df_dtype}
            
            if dtype_mismatches:
                failed_file['dtype_mismatches'] = dtype_mismatches
                # Attempt to convert dtypes
                for col, dtypes in dtype_mismatches.items():
                    try:
                        target_dtype = self.map_parquet_dtype_to_pandas(dtypes['parquet_dtype'])
                        df[col] = df[col].astype(target_dtype)
                        self.logger.info(f"Converted column '{col}' to {target_dtype}.")
                    except Exception as e:
                        self.logger.error(f"Failed to convert column '{col}' to {dtypes['parquet_dtype']}. Reason: {e}")
                        return  # Skip appending if conversion fails

            # Save corrected file
            ddf = dd.from_pandas(df, npartitions=1)
            name_function = lambda x: f"{os.path.splitext(failed_file['file_name'])[0]}-{x}.parquet"
            ddf.to_parquet(
                self.output_dir,
                engine='pyarrow',
                compression='snappy',
                append=True,
                ignore_divisions=False,
                write_index=False,
                name_function=name_function
            )
            self.logger.info(f'Successfully reprocessed shapefile {failed_file["file_name"]}')
            # Remove the file from failed_files as it's now corrected
            self.failed_files.remove(failed_file)
        except Exception as e:
            self.logger.error(f'Failed to correct and process shapefile {failed_file["file_name"]}. Reason: {e}')

    def map_parquet_dtype_to_pandas(self, parquet_dtype: str) -> str:
        """
        Maps Parquet data types to Pandas data types for conversion.
        This mapping might need to be adjusted based on actual data types used.
        """
        mapping = {
            'int32': 'int32',
            'int64': 'int64',
            'float32': 'float32',
            'float64': 'float64',
            'string': 'str',
            'binary': 'bytes',
            # Add more mappings as needed
        }
        return mapping.get(parquet_dtype, 'object')  # Default to 'object' if type not found

    def correct_all_failed_files(self):
        if not self.failed_files:
            self.logger.info("No failed files to correct.")
            return
        for failed_file in self.failed_files.copy():  # Use copy to modify the list while iterating
            self.correct_failed_file(failed_file)

import math

class ParquetConcatenator:
    def __init__(self, output_dir: str, logger: logging.Logger, chunk_size_mb: int = 500):
        self.output_dir = output_dir
        self.logger = logger
        self.chunk_size_mb = chunk_size_mb

    def get_total_size(self, parquet_paths: List[str]) -> int:
        """Returns the total size in bytes of all parquet files."""
        total_size = 0
        for path in parquet_paths:
            total_size += os.path.getsize(path)
        return total_size

import os
import math
import logging
import dask.dataframe as dd
from typing import List, Dict

class ParquetConcatenator:
    def __init__(self, output_dir: str, logger: logging.Logger, chunk_size_mb: int = 200):
        self.output_dir = output_dir
        self.logger = logger
        self.chunk_size_mb = chunk_size_mb

    def get_total_size(self, parquet_paths: List[str]) -> int:
        """Returns the total size in bytes of all parquet files."""
        total_size = 0
        for path in parquet_paths:
            total_size += os.path.getsize(path)
        return total_size

    def concat_parquets_in_chunks(self):
        parquet_files = [f for f in os.listdir(self.output_dir) if f.endswith('.parquet')]
        if not parquet_files:
            self.logger.error('No Parquet files found to concatenate.')
            return
        
        parquet_paths = [os.path.join(self.output_dir, f) for f in parquet_files]
        self.logger.info(f'Found {len(parquet_paths)} Parquet files for concatenation.')

        total_size = self.get_total_size(parquet_paths)
        chunk_size_bytes = self.chunk_size_mb * 1024 * 1024
        num_chunks = math.ceil(total_size / chunk_size_bytes)

        self.logger.info(f'Concatenating into {num_chunks} chunks of approximately {self.chunk_size_mb} MB each.')

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

        # Process each chunk iteratively
        concatenated_path = os.path.join(self.output_dir, 'concatenated.parquet')
        

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

import os
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List
import logging
from dask.distributed import Client, as_completed
def consolidate_single_chunk(chunk_folder: str, output_dir: str) -> str:
    """
    Consolidates Parquet files within a single chunk folder.

    :param chunk_folder: Path to the chunk folder containing Parquet files.
    :param output_dir: Directory where the consolidated file will be saved.
    :param log_dir: Directory where logs are stored.
    :return: Path to the consolidated Parquet file or a status message.
    """
    # Initialize a logger for the task
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    
    chunk_name = os.path.basename(chunk_folder)
    logger.info(f"Consolidating chunk folder: {chunk_name}")

    parquet_files = [os.path.join(chunk_folder, f) for f in os.listdir(chunk_folder) if f.endswith('.parquet')]
    if not parquet_files:
        logger.warning(f"No Parquet files found in {chunk_folder}. Skipping.")
        return f"Skipped {chunk_name}: No Parquet files."

    # Read all Parquet files in the current chunk folder
    tables = []
    for file in parquet_files:
        try:
            logger.debug(f"Reading Parquet file: {file}")
            table = pq.read_table(file)
            tables.append(table)
        except Exception as e:
            logger.error(f"Failed to read Parquet file {file}. Reason: {e}")

    if not tables:
        logger.warning(f"No valid Parquet tables found in {chunk_folder}. Skipping.")
        return f"Skipped {chunk_name}: No valid Parquet tables."

    # Concatenate all tables in the current chunk folder
    try:
        combined_table = pa.concat_tables(tables)
        logger.info(f"Successfully concatenated {len(tables)} Parquet files in {chunk_name}")
    except Exception as e:
        logger.error(f"Failed to concatenate tables in {chunk_name}. Reason: {e}")
        return f"Failed {chunk_name}: Concatenation error."

    # Optionally, add a column to indicate the chunk group
    try:
        chunk_group_column = pa.array([chunk_name] * combined_table.num_rows, type=pa.string())
        combined_table = combined_table.append_column('chunk_group', chunk_group_column)
        logger.debug(f"Added 'chunk_group' column to the combined table for {chunk_name}")
    except Exception as e:
        logger.error(f"Failed to add 'chunk_group' column for {chunk_name}. Reason: {e}")

    # Save the consolidated table for this chunk
    consolidated_file = os.path.join(output_dir, f"{chunk_name}_consolidated.parquet")
    try:
        pq.write_table(combined_table, consolidated_file, compression='snappy')
        logger.info(f"Successfully wrote consolidated Parquet file: {consolidated_file}")
        return consolidated_file
    except Exception as e:
        logger.error(f"Failed to write consolidated Parquet file {consolidated_file}. Reason: {e}")
        return f"Failed {chunk_name}: Write error."
    
class ParquetConsolidator:
    def __init__(self, output_dir: str, logger: logging.Logger, client: Client):
        """
        Initializes the ParquetConsolidator.

        :param output_dir: Directory where concatenated_chunks folders are located.
        :param logger: Logger instance for logging.
        :param client: Dask Client instance for submitting tasks.
        """
        self.output_dir = output_dir
        self.logger = logger
        self.client = client

    def consolidate_all_chunks(self):
        """
        Consolidates Parquet files from all concatenated_chunks folders into a single consolidated.parquet file.
        Utilizes Dask Futures for parallel processing.
        """
        concatenated_chunks_dir = os.path.join(self.output_dir, 'concatenated_chunks')
        if not os.path.exists(concatenated_chunks_dir):
            self.logger.error(f"concatenated_chunks directory does not exist: {concatenated_chunks_dir}")
            return

        # List all chunk folders (e.g., chunk_1, chunk_2, ..., chunk_36)
        chunk_folders = [os.path.join(concatenated_chunks_dir, d) for d in os.listdir(concatenated_chunks_dir)
                         if os.path.isdir(os.path.join(concatenated_chunks_dir, d))]

        self.logger.info(f"Found {len(chunk_folders)} chunk folders to consolidate.")

        if not chunk_folders:
            self.logger.warning("No chunk folders found to consolidate.")
            return

        # Submit consolidation tasks as futures
        futures = []
        for chunk_folder in chunk_folders:
            future = self.client.submit(consolidate_single_chunk, chunk_folder, self.output_dir)
            futures.append(future)
            self.logger.debug(f"Submitted consolidation task for {chunk_folder}")

        # Monitor task progress
        for future, result in as_completed(futures, with_results=True):
            if isinstance(result, Exception):
                self.logger.error(f"Task failed with exception: {result}")
            else:
                self.logger.info(f"Task completed successfully: {result}")

        # After all tasks are done, concatenate all consolidated tables
        self.concatenate_final_table()


    def concatenate_final_table(self):
        """
        Concatenates all individual consolidated Parquet files into a single 'consolidated.parquet' file.
        """
        consolidated_files = [os.path.join(self.output_dir, f) for f in os.listdir(self.output_dir)
                             if f.endswith('_consolidated.parquet')]

        if not consolidated_files:
            self.logger.error("No consolidated Parquet files found to create final consolidated.parquet.")
            return

        self.logger.info(f"Concatenating {len(consolidated_files)} consolidated Parquet files into final consolidated.parquet.")

        tables = []
        for file in consolidated_files:
            try:
                self.logger.debug(f"Reading consolidated Parquet file: {file}")
                table = pq.read_table(file)
                tables.append(table)
            except Exception as e:
                self.logger.error(f"Failed to read consolidated Parquet file {file}. Reason: {e}")

        if not tables:
            self.logger.error("No valid consolidated tables found to create final consolidated.parquet.")
            return

        # Concatenate all tables into a final table
        try:
            final_table = pa.concat_tables(tables)
            self.logger.info(f"Total rows in final consolidated table: {final_table.num_rows}")
        except Exception as e:
            self.logger.error(f"Failed to concatenate all tables into final_table. Reason: {e}")
            return

        # Write the final consolidated Parquet file
        final_consolidated_file = os.path.join(self.output_dir, "consolidated.parquet")
        try:
            pq.write_table(final_table, final_consolidated_file, compression='snappy')
            self.logger.info(f"Successfully wrote final consolidated Parquet file: {final_consolidated_file}")
        except Exception as e:
            self.logger.error(f"Failed to write final consolidated Parquet file {final_consolidated_file}. Reason: {e}")


# Updated WorkflowManager Class
class WorkflowManager:
    def __init__(self, input_dir: str, output_dir: str, log_dir: str, file_name_selectors: List[str] = None, client: Client = None):
        self.logger_instance = Logger(log_dir)
        self.logger = self.logger_instance.get_logger()
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.file_name_selectors = file_name_selectors if file_name_selectors else []
        self.shapefile_processor = ShapefileProcessor(input_dir, output_dir, self.logger, self.file_name_selectors)
        self.failed_file_corrector = FailedFileCorrector(input_dir, output_dir, self.logger, self.shapefile_processor.failed_files)
        self.parquet_concatenator = ParquetConcatenator(output_dir, self.logger)
        self.parquet_consolidator = ParquetConsolidator(output_dir, self.logger, client) 

    def run_step1_process_shapefiles(self):
        self.logger.info("=== Step 1: Processing Shapefiles ===")
        failed_files = self.shapefile_processor.process_all_shapefiles()
        self.logger.info(f"Step 1 completed with {len(failed_files)} failed files.")

    def run_step2_correct_failed_files(self):
        self.logger.info("=== Step 2: Correcting Failed Files ===")
        self.failed_file_corrector.correct_all_failed_files()
        remaining_failures = len(self.failed_file_corrector.failed_files)
        self.logger.info(f"Step 2 completed with {remaining_failures} remaining failed files.")

    def run_step3_concatenate_parquets(self):
        self.logger.info("=== Step 3: Concatenating Parquet Files ===")
        self.parquet_concatenator.concat_parquets_in_chunks()
        self.logger.info("Step 3 completed.")

    def run_step4_consolidate_parquets(self):
        self.logger.info("=== Step 4: Consolidating Parquet Files ===")
        self.parquet_consolidator.consolidate_all_chunks()
        self.logger.info("Step 4 (Consolidation) completed.")

    def save_failed_files_log(self):
        failed_files = self.shapefile_processor.failed_files
        if not failed_files:
            self.logger.info("No failed files to log.")
            return
        failed_files_df = pd.DataFrame(failed_files)
        failed_files_df.to_csv(os.path.join(self.logger_instance.log_dir, 'failed_files.csv'), index=False)
        self.logger.info(f'Saved failed files log to {os.path.join(self.logger_instance.log_dir, "failed_files.csv")}')

    def run(self, steps: List[int]):
        for step in steps:
            if step == 1:
                self.run_step1_process_shapefiles()
            elif step == 2:
                self.run_step2_correct_failed_files()
            elif step == 3:
                self.run_step3_concatenate_parquets()
            elif step == 4:
                self.run_step4_consolidate_parquets()
            else:
                self.logger.error(f'Invalid step: {step}. Choose from [1, 2, 3, 4].')
        self.save_failed_files_log()

from dask.distributed import Client, progress

# Initialize Dask Client
def initialize_dask_client(n_workers: int = None, threads_per_worker: int = None, memory_limit: str = '2GB'):
    """
    Initializes a Dask distributed client.

    :param n_workers: Number of worker processes. If None, defaults to number of cores.
    :param threads_per_worker: Number of threads per worker. If None, defaults to 1.
    :param memory_limit: Memory limit per worker (e.g., '2GB').
    :return: Dask Client instance.
    """
    try:
        client = Client(n_workers=n_workers, threads_per_worker=threads_per_worker, memory_limit=memory_limit)
        print(f"Dask Client initialized: {client}")
        return client
    except Exception as e:
        print(f"Failed to initialize Dask Client. Reason: {e}")
        raise


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
    output_dir = config.get("output_dir")
    log_dir = config.get("log_dir")
    file_name_selectors = config.get("file_name_selectors")
    steps = config.get("steps")

    # Example usage of configuration values
    print(f"Input Directory: {input_dir}")
    print(f"Output Directory: {output_dir}")
    print(f"Log Directory: {log_dir}")
    print(f"File Name Selectors: {file_name_selectors}")
    print(f"Steps to Execute: {steps}")
    # Initialize Dask Client
    client = initialize_dask_client()
    workflow_manager = WorkflowManager(
        input_dir=os.path.join(wkdir, input_dir),
        output_dir=os.path.join(wkdir, output_dir),
        log_dir=os.path.join(wkdir, log_dir),
        file_name_selectors=file_name_selectors,
        client=client
    )

    workflow_manager.run(steps=steps)
    # Optionally, close the Dask client
    client.close()

if __name__ == '__main__':
    main()