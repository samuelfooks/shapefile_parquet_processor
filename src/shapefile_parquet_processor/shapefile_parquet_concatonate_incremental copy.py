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

def shapefile_to_gdf(file_path: str, crs: str = 'EPSG:4326') -> gpd.GeoDataFrame:
    """
    Reads a shapefile into a GeoDataFrame and converts its coordinate reference system (CRS).

    :param file_path: Path to the shapefile.
    :param crs: Target CRS to convert the shapefile into.
    :return: GeoDataFrame containing the shapefile data.
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

    def list_parquet_files(self) -> list:
        """
        Lists all the Parquet files in the specified directory.

        :return: List of Parquet file paths.
        """
        return [os.path.join(self.parquet_dir, file) for file in os.listdir(self.parquet_dir) if file.endswith('.parquet')]

    def align_table_to_schema(self, table: pa.Table, target_schema: pa.Schema) -> pa.Table:
        """
        Aligns a PyArrow table to a target schema by adding missing columns with nulls.
        
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
                        # Convert string columns to timestamp if necessary
                        if pa.types.is_string(column.type) and pa.types.is_timestamp(field.type):
                            column = pa.array(pd.to_datetime(column.to_pandas(), errors='coerce'))
                        else:
                            column = column.cast(field.type)
                    except Exception as e:
                        logging.error(f'Error converting column {field.name}: {e}')
                        return None
                else:
                    # Create a null column with the target type
                    column = pa.nulls(table.num_rows, type=field.type)
                new_columns.append(column)
            return pa.Table.from_arrays(new_columns, schema=target_schema)
        except Exception as e:
            logging.error(f'Error aligning table to schema: {e}')
            return None

    def make_batches(self):
        parquet_files = self.list_parquet_files()
        if not parquet_files:
            logging.warning(f'No Parquet files found in {self.parquet_dir}')
            return

        batch_tables = []
        batch_columns = set()
        batch_size = 0
        batch_count = 1
        processed_files = 0
        total_files = len(parquet_files)

        for file in parquet_files:
            logging.info(f'Processing Parquet file: {file}')
            try:
                single_pq = pq.read_table(file)
            except Exception as e:
                logging.error(f'Error reading Parquet file {file}: {e}')
                continue

            # Update batch columns with the current table's columns
            batch_columns.update(single_pq.schema.names)
            batch_tables.append(single_pq)
            batch_size += single_pq.nbytes

            # Check if batch size limit is reached or it's the last file
            if batch_size >= self.max_batch_size_bytes or processed_files == total_files - 1:
                # Determine the union schema for the batch by merging all table schemas
                union_schema = self.unify_schemas(batch_tables)

                # Align each table to the union schema
                aligned_tables = []
                for table in batch_tables:
                    aligned_table = self.align_table_to_schema(table, union_schema)
                    # Skip unaligned tables
                    if aligned_table is not None:
                        aligned_tables.append(aligned_table)
                    else:
                        logging.warning(f'Skipping unaligned table from file: {file}')

                try:
                    # Concatenate aligned tables
                    concatenated_table = pa.concat_tables(aligned_tables, promote=True)
                    if self.make_batch_parquet(concatenated_table, batch_count):
                        logging.info(f'Batch {batch_count} written successfully')
                        batch_count += 1
                        batch_tables = []
                        batch_columns = set()
                        batch_size = 0
                    else:
                        logging.error(f'Error writing batch {batch_count}')
                except Exception as e:
                    logging.error(f'Error concatenating tables for batch {batch_count}: {e}')

            processed_files += 1

        logging.info('All Parquet files processed.')

    def unify_schemas(self, tables: list) -> pa.Schema:
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
            if pa.types.is_string(type1) or pa.types.is_string(type2):
                return pa.string()
            if pa.types.is_floating(type1) or pa.types.is_floating(type2):
                return pa.float64()
            if pa.types.is_integer(type1) and pa.types.is_integer(type2):
                return pa.int64()
            if pa.types.is_timestamp(type1) and pa.types.is_timestamp(type2):
                return pa.timestamp('ns')
            if pa.types.is_timestamp(type1) or pa.types.is_timestamp(type2):
                return pa.timestamp('ns')
            # Add more type promotion rules as needed
            raise ValueError(f"Cannot promote types: {type1} and {type2}")

        # Collect all fields from all tables
        all_fields = {}
        for table in tables:
            for field in table.schema:
                if field.name in all_fields:
                    # Promote the type if necessary
                    existing_type = all_fields[field.name]
                    if existing_type != field.type:
                        # Handle type promotion
                        try:
                            promoted_type = promote_types(existing_type, field.type)
                            all_fields[field.name] = promoted_type
                        except ValueError as ve:
                            logging.error(ve)
                            # Optionally, handle the conflict or skip the field
                            continue
                else:
                    all_fields[field.name] = field.type

        # Create a unified schema
        unified_fields = [pa.field(name, dtype) for name, dtype in sorted(all_fields.items())]
        unified_schema = pa.schema(unified_fields)
        return unified_schema

    def make_batch_parquet(self, batch_table: pa.Table, batch_count: int) -> bool:
        """
        Writes a PyArrow Table to a Parquet file.

        :param batch_table: PyArrow Table to write.
        :param batch_count: Index of the batch.
        :return: True if the table is successfully written, False otherwise.
        """
        try:
            batch_path = os.path.join(self.parquet_dir, f'batch_{batch_count}.parquet')
            logging.info(f'Writing batch {batch_count} to {batch_path}')
            pq.write_table(batch_table, batch_path, compression='snappy')
            return True
        except Exception as e:
            logging.error(f'Error writing batch {batch_count}: {e}')
            return False

    def validate_parquet_files(self, parquet_files: list) -> list:
        """
        Validates Parquet files to ensure they can be read and have consistent schemas.

        :param parquet_files: List of Parquet file paths.
        :return: List of valid Parquet file paths.
        """
        valid_files = []
        reference_schema = None

        for file in parquet_files:
            logging.info(f'Validating batch file: {file}')
            try:
                table = pq.read_table(file)
                if reference_schema is None:
                    reference_schema = table.schema
                    logging.debug(f'Set reference schema from {file}.')
                else:
                    if not table.schema.equals(reference_schema, check_metadata=True):
                        logging.warning(f'Schema mismatch in {file}. Expected {reference_schema}, got {table.schema}. Skipping.')
                        continue
                valid_files.append(file)
                logging.debug(f'{file} is valid and matches the reference schema.')
            except Exception as e:
                logging.error(f'Failed to validate {file}: {e}')
                continue

        logging.info(f'Validation complete. {len(valid_files)} out of {len(parquet_files)} files are valid.')
        return valid_files

    def concat_batch_parquet_files_incremental(self, parquet_files: list):
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


    def clean_up_batches(self, parquet_files: list):
        """
        Deletes or archives batch Parquet files after successful concatenation.

        :param parquet_files: List of batch Parquet file paths.
        :return: None
        """
        for file in parquet_files:
            try:
                os.remove(file)
                logging.debug(f'Deleted batch file: {file}')
            except Exception as e:
                logging.error(f'Failed to delete {file}: {e}')
            

    def make_batches(self):
        parquet_files = self.list_parquet_files()
        if not parquet_files:
            logging.warning(f'No Parquet files found in {self.parquet_dir}')
            return

        batch_tables = []
        batch_columns = set()
        batch_size = 0
        batch_count = 1
        processed_files = 0
        total_files = len(parquet_files)

        for file in parquet_files:
            logging.info(f'Processing Parquet file: {file}')
            try:
                single_pq = pq.read_table(file)
            except Exception as e:
                logging.error(f'Error reading Parquet file {file}: {e}')
                continue

            # Update batch columns with the current table's columns
            batch_columns.update(single_pq.schema.names)
            batch_tables.append(single_pq)
            batch_size += single_pq.nbytes

            # Check if batch size limit is reached or it's the last file
            if batch_size >= self.max_batch_size_bytes or processed_files == total_files - 1:
                # Determine the union schema for the batch by merging all table schemas
                union_schema = self.unify_schemas(batch_tables)

                # Align each table to the union schema
                aligned_tables = []
                for table in batch_tables:
                    aligned_table = self.align_table_to_schema(table, union_schema)
                    # Skip unaligned tables
                    if aligned_table is not None:
                        aligned_tables.append(aligned_table)
                    else:
                        logging.warning(f'Skipping unaligned table from file: {file}')

                try:
                    # Concatenate aligned tables
                    concatenated_table = pa.concat_tables(aligned_tables, promote=True)
                    if self.make_batch_parquet(concatenated_table, batch_count):
                        logging.info(f'Batch {batch_count} written successfully')
                        batch_count += 1
                        batch_tables = []
                        batch_columns = set()
                        batch_size = 0
                    else:
                        logging.error(f'Error writing batch {batch_count}')
                except Exception as e:
                    logging.error(f'Error concatenating tables for batch {batch_count}: {e}')

            processed_files += 1

        logging.info('All Parquet files processed.')

    def unify_schemas(self, tables: list) -> pa.Schema:
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
            if pa.types.is_string(type1) or pa.types.is_string(type2):
                return pa.string()
            if pa.types.is_floating(type1) or pa.types.is_floating(type2):
                return pa.float64()
            if pa.types.is_integer(type1) and pa.types.is_integer(type2):
                return pa.int64()
            if pa.types.is_timestamp(type1) and pa.types.is_timestamp(type2):
                return pa.timestamp('ns')
            if pa.types.is_timestamp(type1) or pa.types.is_timestamp(type2):
                return pa.timestamp('ns')
            # Add more type promotion rules as needed
            raise ValueError(f"Cannot promote types: {type1} and {type2}")

        # Collect all fields from all tables
        all_fields = {}
        for table in tables:
            for field in table.schema:
                if field.name in all_fields:
                    # Promote the type if necessary
                    existing_type = all_fields[field.name]
                    if existing_type != field.type:
                        # Handle type promotion
                        promoted_type = promote_types(existing_type, field.type)
                        all_fields[field.name] = promoted_type
                else:
                    all_fields[field.name] = field.type

        # Create a unified schema
        unified_fields = [pa.field(name, dtype) for name, dtype in sorted(all_fields.items())]
        unified_schema = pa.schema(unified_fields)
        return unified_schema
    def make_batch_parquet(self, batch_table: pa.Table, batch_count: int) -> bool:
        """
        Writes a PyArrow Table to a Parquet file.

        :param batch_table: PyArrow Table to write.
        :param batch_count: Index of the batch.
        :return: True if the table is successfully written, False otherwise.
        """
        try:
            batch_path = os.path.join(self.parquet_dir, f'batch_{batch_count}.parquet')
            logging.info(f'Writing batch {batch_count} to {batch_path}')
            pq.write_table(batch_table, batch_path, compression='snappy')
            return True
        except Exception as e:
            logging.error(f'Error writing batch {batch_count}: {e}')
            return False

    def concat_batch_parquet_files(self, parquet_files: list):
        """
        Concatenates multiple Parquet files into a single file,
        while ensuring consistent data types across files.

        :return: None
        """
        try:
            parquet_batches = []
            if not parquet_files:
                logging.warning('No Parquet files to concatenate.')
                return

            for parquet_file in parquet_files:
                if 'batch' in parquet_file:
                    parquet_batches.append(parquet_file)

            if not parquet_batches:
                logging.warning('No Parquet batch files found.')
                return

            logging.info(f'Concatenating {len(parquet_batches)} Parquet batch files')
            dfs = [dd.read_parquet(file, index=False) for file in parquet_batches]

            combined_df = dd.concat(dfs)

            # Reset the index to avoid issues with the index column
            combined_df = combined_df.reset_index(drop=True)

            # Compute the combined DataFrame to convert it to a Pandas DataFrame
            combined_df = combined_df.compute()

            # Extract the schema from the combined DataFrame
            combined_schema = pa.Schema.from_pandas(combined_df, preserve_index=False)

            # Create a PyArrow Table from the Pandas DataFrame
            combined_table = pa.Table.from_pandas(combined_df, schema=combined_schema, preserve_index=False)

            logging.info(f'Writing concatenated DataFrame to {self.output_parquet}')
            pq.write_table(combined_table, self.output_parquet, compression='snappy')
            logging.info('Parquet concatenation completed.')
        except Exception as e:
            logging.error(f'Error concatenating Parquet files: {e}')


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

    def list_shapefiles(self, country_codes: list) -> list:
        """
        Lists shapefiles filtered by country codes if provided.

        :param country_codes: List of country codes to filter the shapefiles.
        :return: List of shapefile paths.
        """
        try:
            all_files = [file for file in os.listdir(self.shp_dir) if file.endswith('.shp')]
            if country_codes:
                filtered_files = [file for file in all_files if file[:2].upper() in [code.upper() for code in country_codes]]
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
            pq.write_table(table, os.path.join(self.parquet_path, f'{os.path.basename(file_path)[:-4]}.parquet'), compression='snappy')
            logging.info(f'Successfully processed shapefile {os.path.basename(file_path)}')
        except Exception as e:
            logging.error(f'Error processing shapefile {file_path}: {e}')

    def run(self, country_codes: list = []):
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
    wkdir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(wkdir, '../data/logs')
    clean_dir(log_dir)

    shp_dir = os.path.join(wkdir, '../data/surveymaps')
    parquet_dir = os.path.join(wkdir, '../data/sbh_survey_parquet')
    output_dir = os.path.join(wkdir, '../data/final_output')
    output_parquet = f"{output_dir}/sbh_survey_concatonated.parquet"
    os.makedirs(parquet_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    clean_dir(parquet_dir)


    # Process shapefiles to Parquet
    processor = ShapefileToParquet(shp_dir, parquet_dir, log_dir)
    processor.run(country_codes=['BE', 'FR', 'DK', 'IT', 'PT'])

    # Concatenate Parquet files
    concatenator = ConcatenateParquet(parquet_dir, output_parquet, max_batch_size_mb=100)
    concatenator.make_batches()
    # List only batch files
    batch_files = [os.path.join(parquet_dir, file) for file in os.listdir(parquet_dir) if file.startswith('batch_') and file.endswith('.parquet')]
    
    # Validate batch files
    valid_batch_files = concatenator.validate_parquet_files(batch_files)
     # Option 2: Incremental concatenation (recommended for large datasets)
    concatenator.concat_batch_parquet_files_incremental(valid_batch_files)

    # Optionally clean up batch files after successful concatenation
    concatenator.clean_up_batches(valid_batch_files)

    logging.info('All processes completed successfully.')

if __name__ == '__main__':
    main()
