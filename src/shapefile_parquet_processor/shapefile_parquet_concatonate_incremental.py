import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import geopandas as gpd
import pyogrio
import dask.dataframe as dd
import shutil
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

def setup_logging(log_dir: str):
    """
    Sets up logging configuration to log events to both a file and the console.

    :param log_dir: Directory path where log files will be stored.
    """
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_dir, 'shapefile_processing.log'),
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s',
        filemode='w'
    )
    logging.getLogger().addHandler(logging.StreamHandler())

def clean_dir(directory: str) -> None:
    """
    Cleans a directory by deleting its contents or the directory itself and recreating it.

    :param directory: Path to the directory to be cleaned.
    """
    if os.path.isdir(directory):
        shutil.rmtree(directory)
    elif os.path.exists(directory):
        os.remove(directory)
    os.makedirs(directory, exist_ok=True)


# Function to clean directories
def clean_dir(directory: str):
    if os.path.exists(directory):
        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                logging.error(f'Error cleaning directory {directory}: {e}')

# Function to read shapefile into GeoDataFrame
def shapefile_to_gdf(file_path: str, crs: str = 'EPSG:4326') -> Optional[gpd.GeoDataFrame]:
    """
    Reads a shapefile into a GeoDataFrame and converts its coordinate reference system (CRS).

    :param file_path: Path to the shapefile.
    :param crs: Target CRS to convert the shapefile into.
    :return: GeoDataFrame containing the shapefile data or None if failed.
    """
    try:
        gdf = pyogrio.read_dataframe(file_path)
        if gdf is None or gdf.empty:
            logging.error(f'Empty or invalid GeoDataFrame for file {file_path}')
            return None
        if gdf.crs != crs:
            logging.info(f'Converting CRS for {file_path} to {crs}')
            gdf = gdf.to_crs(crs)
        gdf = gdf.reset_index(drop=True)
        return gdf
    except Exception as e:
        logging.error(f'Error reading shapefile {file_path}: {e}')
        return None

class ConcatenateParquet:
    """
    A class responsible for concatenating Parquet files into a single file,
    while ensuring that the data types are consistent across files.

    :param parquet_dir: Directory where Parquet files are located.
    :param output_parquet: Path to the output concatenated Parquet file.
    :param max_batch_size_mb: Maximum size of each batch in megabytes.
    """

    def __init__(self, parquet_dir: str, output_parquet: str, max_batch_size_mb: int = 100):
        self.parquet_dir = parquet_dir
        self.output_parquet = output_parquet
        self.max_batch_size_bytes = max_batch_size_mb * 1024 * 1024

    def list_parquet_files(self) -> List[str]:
        """
        Lists all the Parquet files in the specified directory.

        :return: List of Parquet file paths.
        """
        return [os.path.join(self.parquet_dir, file) for file in os.listdir(self.parquet_dir) if file.endswith('.parquet')]

    def determine_unified_schema(self, parquet_files: List[str]) -> Optional[pa.Schema]:
        """
        Determines a unified schema by scanning all Parquet files and promoting types as necessary.

        :param parquet_files: List of Parquet file paths.
        :return: Unified PyArrow schema or None if unable to determine.
        """
        all_tables = []
        for file in parquet_files:
            try:
                table = pq.read_table(file)
                all_tables.append(table)
                logging.info(f'Read schema from {file}')
            except Exception as e:
                logging.error(f'Error reading Parquet file {file} for schema determination: {e}')
                continue

        if not all_tables:
            logging.error('No valid Parquet files found for schema determination.')
            return None

        try:
            unified_schema = self.unify_schemas(all_tables)
            logging.info('Unified schema determined successfully.')
            return unified_schema
        except Exception as e:
            logging.error(f'Error determining unified schema: {e}')
            return None

    def unify_schemas(self, tables: List[pa.Table]) -> pa.Schema:
        """
        Creates a unified schema by taking the union of all schemas in the provided tables.
        If a column appears in multiple tables with different types, promotes the type to accommodate all.

        :param tables: List of PyArrow tables.
        :return: Unified PyArrow schema.
        """
        def promote_types(type1, type2):
            """
            Promotes two PyArrow types to a common type.

            :param type1: First PyArrow type.
            :param type2: Second PyArrow type.
            :return: Promoted PyArrow type.
            """
            # Handle specific cases first
            # Promote timestamp[ms] to timestamp[ns]
            if pa.types.is_timestamp(type1) and pa.types.is_timestamp(type2):
                if type1.unit == 'ms' or type2.unit == 'ms':
                    return pa.timestamp('ns')
                else:
                    return pa.timestamp('ns')  # Default to ns

            # Promote integers and floats
            if pa.types.is_integer(type1) and pa.types.is_integer(type2):
                return pa.int64()
            if pa.types.is_floating(type1) or pa.types.is_floating(type2):
                return pa.float64()

            # Handle string promotions
            if pa.types.is_string(type1) or pa.types.is_string(type2):
                return pa.string()

            # Handle incompatible types by promoting to string
            return pa.string()
        logging.info('Unifying schemas from all tables.')
        all_fields = {}
        for table in tables:
            for field in table.schema:
                if field.name in all_fields:
                    existing_type = all_fields[field.name]
                    if existing_type != field.type:
                        logging.info(f'Found conflicting types for column "{field.name}": {existing_type} and {field.type}')
                        try:
                            logging.info(f'Attempting to promote types for column "{field.name}"')
                            promoted_type = promote_types(existing_type, field.type)
                            all_fields[field.name] = promoted_type
                            logging.info(f'Promoted type for column "{field.name}" to {promoted_type}')
                        except ValueError as ve:
                            logging.error(ve)
                            # Fallback to string if promotion fails
                            all_fields[field.name] = pa.string()
                            logging.info(f'Falling back to string type for column "{field.name}"')
                else:

                    all_fields[field.name] = field.type

        unified_fields = [pa.field(name, dtype) for name, dtype in sorted(all_fields.items())]
        unified_schema = pa.schema(unified_fields)
        return unified_schema

    def align_table_to_schema(self, table: pa.Table, target_schema: pa.Schema) -> Optional[pa.Table]:
        """
        Aligns a PyArrow table to a target schema by adding missing columns with nulls and casting types.

        :param table: The original PyArrow table.
        :param target_schema: The target PyArrow schema.
        :return: Aligned PyArrow table or None if alignment fails.
        """
        try:
            new_columns = []
            for field in target_schema:
                if field.name in table.schema.names:
                    column = table.column(field.name)
                    try:
                        # Handle specific type promotions
                        if pa.types.is_timestamp(column.type) and pa.types.is_timestamp(field.type):
                            # Promote timestamp precision if necessary
                            if column.type.unit != field.type.unit:
                                column = column.cast(field.type)
                                logging.debug(f'Column "{field.name}" timestamp unit promoted to {field.type.unit}')
                        elif pa.types.is_floating(column.type) and pa.types.is_integer(field.type):
                            # Promote float to integer if possible (not recommended usually)
                            column = column.cast(field.type)
                            logging.debug(f'Column "{field.name}" float cast to {field.type}')
                        elif (pa.types.is_floating(column.type) and pa.types.is_string(field.type)) or \
                             (pa.types.is_integer(column.type) and pa.types.is_string(field.type)):
                            # Cast numerical types to string
                            pandas_series = column.to_pandas().astype(str)
                            column = pa.array(pandas_series, type=pa.string())
                            logging.debug(f'Column "{field.name}" cast from numerical to string')
                        elif pa.types.is_integer(column.type) and pa.types.is_floating(field.type):
                            # Promote integer to float
                            column = column.cast(field.type)
                            logging.debug(f'Column "{field.name}" integer promoted to {field.type}')
                        else:
                            # General cast
                            column = column.cast(field.type)
                            logging.debug(f'Column "{field.name}" cast to {field.type}')
                    except Exception as e:
                        logging.error(f'Error converting column "{field.name}": {e}')
                        # Fallback to casting to string if casting fails
                        try:
                            pandas_series = column.to_pandas().astype(str)
                            column = pa.array(pandas_series, type=pa.string())
                            logging.info(f'Column "{field.name}" cast to string as fallback')
                        except Exception as fallback_e:
                            logging.error(f'Fallback casting failed for column "{field.name}": {fallback_e}')
                            return None
                else:
                    # Create a null column with the target type
                    column = pa.nulls(table.num_rows, type=field.type)
                    logging.debug(f'Added null column for missing field "{field.name}"')
                new_columns.append(column)
            aligned_table = pa.Table.from_arrays(new_columns, schema=target_schema)
            return aligned_table
        except Exception as e:
            logging.error(f'Error aligning table to schema: {e}')
            return None

    def make_batches(self, unified_schema: pa.Schema) -> List[str]:
        """
        Creates batches of Parquet files aligned to the unified schema and writes them as batch Parquet files.

        :param unified_schema: The unified PyArrow schema to align all tables.
        :return: List of batch Parquet file paths.
        """
        parquet_files = self.list_parquet_files()
        if not parquet_files:
            logging.warning(f'No Parquet files found in {self.parquet_dir}')
            return []

        batch_tables = []
        batch_size = 0
        batch_count = 1
        batch_file_paths = []

        for file in parquet_files:
            logging.info(f'Processing Parquet file: {file}')
            try:
                table = pq.read_table(file)
                aligned_table = self.align_table_to_schema(table, unified_schema)
                if aligned_table is None:
                    logging.warning(f'Skipping file due to alignment failure: {file}')
                    continue
                batch_tables.append(aligned_table)
                batch_size += aligned_table.nbytes

                # Check if batch size limit is reached
                if batch_size >= self.max_batch_size_bytes:
                    concatenated_table = pa.concat_tables(batch_tables, promote=True)
                    batch_path = self.make_batch_parquet(concatenated_table, batch_count)
                    if batch_path:
                        batch_file_paths.append(batch_path)
                        logging.info(f'Batch {batch_count} written successfully with size {batch_size} bytes.')
                        batch_count += 1
                        batch_tables = []
                        batch_size = 0
            except Exception as e:
                logging.error(f'Error processing file {file}: {e}')
                continue

        # Handle any remaining tables in the last batch
        if batch_tables:
            try:
                concatenated_table = pa.concat_tables(batch_tables, promote=True)
                batch_path = self.make_batch_parquet(concatenated_table, batch_count)
                if batch_path:
                    batch_file_paths.append(batch_path)
                    logging.info(f'Final batch {batch_count} written successfully with size {batch_size} bytes.')
            except Exception as e:
                logging.error(f'Error writing final batch {batch_count}: {e}')

        logging.info('All batches created successfully.')
        return batch_file_paths

    def make_batch_parquet(self, batch_table: pa.Table, batch_count: int) -> Optional[str]:
        """
        Writes a PyArrow Table to a Parquet file.

        :param batch_table: PyArrow Table to write.
        :param batch_count: Index of the batch.
        :return: Path to the written batch Parquet file or None if failed.
        """
        try:
            batch_path = os.path.join(self.parquet_dir, f'batch_{batch_count}.parquet')
            logging.info(f'Writing batch {batch_count} to {batch_path}')
            pq.write_table(batch_table, batch_path, compression='snappy')
            return batch_path
        except Exception as e:
            logging.error(f'Error writing batch {batch_count}: {e}')
            return None

    def concat_batch_parquet_files_incremental(self, parquet_files: List[str]):
        """
        Concatenates multiple Parquet batch files into a single Parquet file
        incrementally to optimize memory usage.

        :param parquet_files: List of batch Parquet file paths.
        :return: None
        """
        if not parquet_files:
            logging.warning('No Parquet batch files to concatenate.')
            return

        logging.info(f'Starting incremental concatenation of {len(parquet_files)} batch files.')

        try:
            # Initialize ParquetWriter with the schema of the first valid table
            writer = None
            reference_schema = None

            for idx, file in enumerate(parquet_files):
                logging.info(f'Reading batch file: {file}')
                try:
                    table = pq.read_table(file)
                    if writer is None:
                        reference_schema = table.schema
                        writer = pq.ParquetWriter(self.output_parquet, table.schema, compression='snappy')
                        logging.debug(f'Initialized ParquetWriter with schema from {file}.')

                    else:
                        if not table.schema.equals(reference_schema, check_metadata=True):
                            logging.warning(f'Schema mismatch in {file}. Expected {reference_schema}, got {table.schema}. Skipping.')
                            continue

                    writer.write_table(table)
                    logging.debug(f'Written {file}: {table.num_rows} rows.')
                except Exception as e:
                    logging.error(f'Failed to read/write {file}: {e}')
                    continue  # Skip problematic files

            if writer:
                writer.close()
                logging.info('Final incremental concatenation completed successfully.')
            else:
                logging.error('No valid tables were processed. Output file not created.')

        except MemoryError:
            logging.error('MemoryError: Insufficient memory to concatenate Parquet files incrementally.')
        except Exception as e:
            logging.error(f'Error during incremental concatenation: {e}')

    def validate_parquet_files(self, parquet_files: List[str], reference_schema: pa.Schema) -> List[str]:
        """
        Validates Parquet files to ensure they can be read and have consistent schemas.

        :param parquet_files: List of Parquet file paths.
        :param reference_schema: The unified schema to validate against.
        :return: List of valid Parquet file paths.
        """
        valid_files = []

        for file in parquet_files:
            logging.info(f'Validating batch file: {file}')
            try:
                table = pq.read_table(file)
                if table.schema.equals(reference_schema, check_metadata=True):
                    valid_files.append(file)
                    logging.debug(f'{file} is valid and matches the reference schema.')
                else:
                    logging.warning(f'Schema mismatch in {file}. Expected {reference_schema}, got {table.schema}. Skipping.')
            except Exception as e:
                logging.error(f'Failed to validate {file}: {e}')
                continue

        logging.info(f'Validation complete. {len(valid_files)} out of {len(parquet_files)} files are valid.')
        return valid_files

class ShapefileToParquet:
    """
    A class to handle the process of listing and processing shapefiles into Parquet format.

    :param shp_dir: Directory containing the shapefiles.
    :param parquet_path: Directory where Parquet files will be saved.
    :param log_dir: Directory for saving log files.
    """

    def __init__(self, shp_dir: str, parquet_path: str, log_dir: str):
        self.shp_dir = shp_dir
        self.parquet_path = parquet_path
        self.log_dir = log_dir
        setup_logging(log_dir)

    def list_shapefiles(self, country_codes: List[str]) -> List[str]:
        """
        Lists shapefiles filtered by country codes if provided.

        :param country_codes: List of country codes to filter the shapefiles.
        :return: List of shapefile paths.
        """
        try:
            all_files = [file for file in os.listdir(self.shp_dir) if file.endswith('.shp')]
            if country_codes:
                filtered_files = [
                    file for file in all_files 
                    if file[:2].upper() in [code.upper() for code in country_codes]
                ]
                logging.info(f'Filtered shapefiles based on country codes: {country_codes}')
            else:
                filtered_files = all_files
                logging.info('No country codes provided. Processing all shapefiles.')

            logging.info(f'Found {len(filtered_files)} shapefiles to process.')
            return [os.path.join(self.shp_dir, file) for file in filtered_files]
        except Exception as e:
            logging.error(f'Error listing shapefiles: {e}')
            return []

    def process_shapefile(self, file_path: str):
        """
        Processes a single shapefile: reads it, converts to Parquet, and writes to the parquet directory.

        :param file_path: Path to the shapefile.
        :return: None
        """
        logging.info(f'Processing shapefile: {os.path.basename(file_path)}')
        gdf = shapefile_to_gdf(file_path)
        if gdf is None or gdf.empty:
            logging.error(f'Failed to process shapefile {file_path}. Skipping.')
            return

        # Convert geometry to WKT and ensure correct data types
        if 'geometry' in gdf.columns:
            try:
                gdf['geometry'] = gdf['geometry'].apply(lambda x: x.wkt if x else None)
                gdf['geometry'] = gdf['geometry'].astype('string')
                gdf = gdf.rename(columns=str.lower)

                # Aligning data types
                for col in gdf.columns:
                    if gdf[col].dtype == 'float64':
                        gdf[col] = gdf[col].astype('float32')
                    elif gdf[col].dtype in ['int64', 'int32']:
                        gdf[col] = gdf[col].astype('float32')  # Consider if this is intended
                    elif gdf[col].dtype == 'object':
                        gdf[col] = gdf[col].astype('string')
            except Exception as e:
                logging.error(f'Error converting geometry for shapefile {file_path}: {e}')
                return

        try:
            logging.info(f'Writing Parquet file for {os.path.basename(file_path)}')
            df = dd.from_pandas(gdf, npartitions=1)
            table = pa.Table.from_pandas(df.compute(), preserve_index=False)
            parquet_file = os.path.join(
                self.parquet_path, f'{os.path.basename(file_path)[:-4]}.parquet'
            )
            pq.write_table(table, parquet_file, compression='snappy')
            logging.info(f'Successfully processed shapefile {os.path.basename(file_path)}')
        except Exception as e:
            logging.error(f'Error processing shapefile {file_path}: {e}')

    def run(self, country_codes: List[str] = []):
        """
        Runs the shapefile processing pipeline by converting shapefiles to Parquet files.

        :param country_codes: List of country codes to filter the shapefiles.
        :return: None
        """
        shapefile_paths = self.list_shapefiles(country_codes)
        if not shapefile_paths:
            logging.warning('No shapefiles to process. Exiting.')
            return
        for path in shapefile_paths:
            self.process_shapefile(path)
        logging.info('Shapefile processing completed.')

def main():
    """
    Main function to set up and run the entire process.
    """
    # Define working directory
    wkdir = os.path.dirname(os.path.abspath(__file__))

    # Define directories
    log_dir = os.path.join(wkdir, '../data/logs')
    clean_dir(log_dir)

    shp_dir = os.path.join(wkdir, '../data/surveymaps')
    parquet_dir = os.path.join(wkdir, '../data/sbh_survey_parquet')
    output_dir = os.path.join(wkdir, '../data/final_output')
    output_parquet = os.path.join(output_dir, 'sbh_survey_concatenated.parquet')

    # Create necessary directories
    os.makedirs(parquet_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    clean_dir(parquet_dir)

    # Process shapefiles to Parquet
    processor = ShapefileToParquet(shp_dir, parquet_dir, log_dir)
    processor.run(country_codes=['IT', 'PT', 'GB'])

    # Concatenate Parquet files
    concatenator = ConcatenateParquet(parquet_dir, output_parquet, max_batch_size_mb=100)

    # List all Parquet files
    all_parquet_files = concatenator.list_parquet_files()
    if not all_parquet_files:
        logging.error('No Parquet files found for concatenation.')
        return

    # Determine unified schema dynamically
    unified_schema = concatenator.determine_unified_schema(all_parquet_files)
    if unified_schema is None:
        logging.error('Failed to determine a unified schema. Exiting.')
        return

    # Make batches aligned to the unified schema
    batch_files = concatenator.make_batches(unified_schema)

    if not batch_files:
        logging.error('No batch files were created. Exiting.')
        return

    # Validate batch files against the unified schema
    valid_batch_files = concatenator.validate_parquet_files(batch_files, unified_schema)

    if not valid_batch_files:
        logging.error('No valid batch files to concatenate. Exiting.')
        return

    # Incrementally concatenate valid batch Parquet files into the final output
    concatenator.concat_batch_parquet_files_incremental(valid_batch_files)

    # Optionally clean up batch files after successful concatenation
    concatenator.clean_up_batches(valid_batch_files)

    logging.info('All processes completed successfully.')

if __name__ == '__main__':
    main()
