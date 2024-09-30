import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import geopandas as gpd
import shutil
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Tuple
# Define the standard schema based on the dominant schema group
# This will be updated dynamically based on the dominant group
STANDARD_SCHEMA = {}

def clean_dir(directory: str):
    """
    Cleans the specified directory by removing all its contents.
    If the directory does not exist, it is created.

    Args:
        directory (str): Path to the directory to clean.
    """
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory, exist_ok=True)

def verify_parquet_file(file_path: str) -> bool:
    """
    Verifies the integrity of a Parquet file.

    Args:
        file_path (str): Path to the Parquet file.

    Returns:
        bool: True if the file is valid, False otherwise.
    """
    try:
        pq.read_table(file_path)
        return True
    except Exception as e:
        logging.error(f'Corrupted Parquet file detected: {file_path} - {e}')
        return False

def remove_corrupted_file(file_path: str):
    """
    Removes a corrupted Parquet file.

    Args:
        file_path (str): Path to the corrupted Parquet file.
    """
    try:
        os.remove(file_path)
        logging.info(f'Removed corrupted file: {file_path}')
    except Exception as e:
        logging.error(f'Error removing file {file_path}: {e}')

class ShapefileToParquet:
    """
    A class to handle the conversion of shapefiles to Parquet format.
    """

    def __init__(self, shp_dir: str, parquet_dir: str, log_dir: str):
        """
        Initializes the ShapefileToParquet processor.

        Args:
            shp_dir (str): Directory containing shapefiles.
            parquet_dir (str): Directory to store Parquet files.
            log_dir (str): Directory to store logs.
        """
        self.shp_dir = shp_dir
        self.parquet_dir = parquet_dir
        self.log_dir = log_dir
        self.schema_records: List[Dict[str, str]] = []  # To store schema info for each Parquet file

        self._setup_logging()

    def _setup_logging(self):
        """
        Sets up logging for the shapefile processing.
        """
        log_file = os.path.join(self.log_dir, 'shapefile_to_parquet.log')
        logging.basicConfig(
            filename=log_file,
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )
        logging.info('Initialized ShapefileToParquet processor.')

    def make_list(self, country_codes: List[str] = None) -> List[str]:
        """
        Generates a list of shapefile paths filtered by country codes.

        Args:
            country_codes (List[str], optional): List of country codes to filter shapefiles. Defaults to None.

        Returns:
            List[str]: List of shapefile paths.
        """
        shapefiles = []
        for file in os.listdir(self.shp_dir):
            if file.endswith('.shp'):
                if country_codes:
                    if any(code in file for code in country_codes):
                        shapefiles.append(os.path.join(self.shp_dir, file))
                else:
                    shapefiles.append(os.path.join(self.shp_dir, file))
        logging.info(f'Found {len(shapefiles)} shapefiles to process.')
        return shapefiles

    def get_shapefile_columns(self, shp_file: str):
        """
        Retrieves and logs the columns of the shapefile.

        Args:
            shp_file (str): Path to the shapefile.
        """
        try:
            gdf = gpd.read_file(shp_file)
            columns = gdf.columns.tolist()
            logging.info(f'Columns for {shp_file}: {columns}')
        except Exception as e:
            logging.error(f'Error reading shapefile {shp_file}: {e}')

    def convert_geometry_to_wkt(self, gdf: gpd.GeoDataFrame) -> pd.DataFrame:
        """
        Converts the geometry column of a GeoDataFrame to WKT strings.

        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame with a geometry column.

        Returns:
            pd.DataFrame: DataFrame with the geometry column converted to WKT.
        """
        if 'geometry' not in gdf.columns:
            logging.warning('No geometry column found to convert to WKT.')
            return gdf

        # Convert geometry to WKT
        gdf['geometry_wkt'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom else None)
        # Optionally, drop the original geometry column
        gdf = gdf.drop(columns=['geometry'])
        return gdf

    def extract_schema(self, gdf: gpd.GeoDataFrame) -> Dict[str, str]:
        """
        Extracts the schema of the GeoDataFrame as a dictionary with lowercase column names and their dtypes.

        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame.

        Returns:
            Dict[str, str]: Dictionary mapping lowercase column names to their data types.
        """
        schema = {col.lower(): str(dtype) for col, dtype in zip(gdf.columns, gdf.dtypes)}
        return schema

    def enforce_standard_schema(self, gdf: gpd.GeoDataFrame, standard_schema: Dict[str, str]) -> pd.DataFrame:
        """
        Enforces the standard schema on the GeoDataFrame by setting data types and handling missing columns.

        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame to enforce the schema on.
            standard_schema (Dict[str, str]): The standard schema to conform to.

        Returns:
            pd.DataFrame: GeoDataFrame with the enforced schema.
        """
        # Rename columns to lowercase
        gdf.columns = [col.lower() for col in gdf.columns]

        # Convert geometry to WKT
        gdf = self.convert_geometry_to_wkt(gdf)

        # Ensure all standard columns are present
        for col, dtype in standard_schema.items():
            if col not in gdf.columns:
                gdf[col] = None  # or appropriate default value
            else:
                # Convert to standard dtype
                if 'datetime' in dtype:
                    gdf[col] = pd.to_datetime(gdf[col], errors='coerce')
                else:
                    try:
                        gdf[col] = gdf[col].astype(dtype)
                    except Exception as e:
                        logging.warning(f'Failed to convert column {col} to {dtype}: {e}')
                        # Optionally, set to None or a default value
                        gdf[col] = gdf[col].astype('object')

        # Reorder columns to match the standard schema
        gdf = gdf[list(standard_schema.keys())]

        return gdf

    def convert_to_parquet(self, shp_file: str, standard_schema: Optional[Dict[str, str]] = None) -> Tuple[bool, str]:
        """
        Converts a shapefile to Parquet format using GeoPandas and PyArrow.
        If a standard schema is provided, enforces it; otherwise, uses the file's schema.

        Args:
            shp_file (str): Path to the shapefile.
            standard_schema (Dict[str, str], optional): The standard schema to enforce. Defaults to None.

        Returns:
            Tuple[bool, str]: (Success status, Error message if any)
        """
        try:
            gdf = gpd.read_file(shp_file)
            logging.info(f'Read shapefile {shp_file} with {len(gdf)} records.')

            # Convert geometry to WKT
            gdf = self.convert_geometry_to_wkt(gdf)
            logging.info(f'Converted geometry to WKT for {shp_file}.')

        
            # Extract and store schema
            schema = self.extract_schema(gdf)
            self.schema_records.append({
                'parquet_file': os.path.splitext(os.path.basename(shp_file))[0] + '.parquet',
                **schema
            })

            # Convert GeoDataFrame to PyArrow Table
            table = pa.Table.from_pandas(gdf, preserve_index=False)
            # Define Parquet file path
            base_name = os.path.splitext(os.path.basename(shp_file))[0]
            parquet_file = os.path.join(self.parquet_dir, f"{base_name}.parquet")
            # Write to Parquet
            pq.write_table(table, parquet_file)
            logging.info(f'Converted {shp_file} to {parquet_file}.')

            return True, ""
        except Exception as e:
            logging.error(f'Error converting {shp_file} to Parquet: {e}')
            return False, str(e)

    def get_schema_dataframe(self) -> pd.DataFrame:
        """
        Creates a pandas DataFrame from the collected schema records.

        Returns:
            pd.DataFrame: DataFrame containing schema information for each Parquet file.
        """
        return pd.DataFrame(self.schema_records)

class ConcatenateParquet:
    """
    A class to handle the concatenation of multiple Parquet files.
    """

    def __init__(self, parquet_dir: str, output_parquet: str, max_batch_size_mb: int = 100):
        """
        Initializes the ConcatenateParquet processor.

        Args:
            parquet_dir (str): Directory containing Parquet files to concatenate.
            output_parquet (str): Path to the final concatenated Parquet file.
            max_batch_size_mb (int, optional): Maximum size of each batch in MB. Defaults to 100.
        """
        self.parquet_dir = parquet_dir
        self.output_parquet = output_parquet
        self.max_batch_size_mb = max_batch_size_mb
        self.batches: Dict[str, List[str]] = {}  # Keyed by schema identifier
        self.failed_files: List[str] = []  # To store failed Parquet files

        self._setup_logging()

    def _setup_logging(self):
        """
        Sets up logging for the Parquet concatenation.
        """
        log_file = os.path.join(os.path.dirname(self.output_parquet), 'concatenate_parquet.log')
        logging.basicConfig(
            filename=log_file,
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )
        logging.info('Initialized ConcatenateParquet processor.')

    def list_parquet_files(self) -> List[str]:
        """
        Lists all Parquet files in the parquet directory.

        Returns:
            List[str]: List of Parquet file paths.
        """
        parquet_files = [
            os.path.join(self.parquet_dir, file)
            for file in os.listdir(self.parquet_dir)
            if file.endswith('.parquet')
        ]
        logging.info(f'Listed {len(parquet_files)} Parquet files for concatenation.')
        return parquet_files

    def group_parquet_files_by_schema_df(self, parquet_files: List[str], schema_df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Groups Parquet files by their schema using a schema DataFrame.

        Args:
            parquet_files (List[str]): List of Parquet file paths.
            schema_df (pd.DataFrame): DataFrame containing schema information for each Parquet file.

        Returns:
            Dict[str, List[str]]: Dictionary grouping file paths by their schema identifier.
        """
        # Create a unique identifier for each schema by concatenating sorted column names and their dtypes
        schema_df_filled = schema_df.fillna('')

        schema_df_filled['schema_identifier'] = schema_df_filled.apply(
            lambda row: ';'.join([f"{col}:{dtype}" for col, dtype in sorted(row.items()) if col != 'parquet_file']),
            axis=1
        )

        # Count unique schemas
        unique_schemas = schema_df_filled['schema_identifier'].unique()
        logging.info(f'Identified {len(unique_schemas)} unique schemas.')

        # Group parquet files by schema_identifier
        schema_groups = {}
        for schema_id in unique_schemas:
            files_in_group = schema_df_filled[schema_df_filled['schema_identifier'] == schema_id]['parquet_file'].tolist()
            # Prepend the parquet_dir to get full paths
            files_full_path = [os.path.join(self.parquet_dir, f) for f in files_in_group]
            schema_groups[schema_id] = files_full_path
            logging.info(f'Group "{schema_id}" has {len(files_full_path)} files.')

        return schema_groups

    def identify_dominant_schema(self, schema_groups: Dict[str, List[str]]) -> Tuple[str, List[str]]:
        """
        Identifies the dominant schema group (the one with the most files).

        Args:
            schema_groups (Dict[str, List[str]]): Dictionary of schema groups.

        Returns:
            Tuple[str, List[str]]: The dominant schema identifier and list of other schema identifiers.
        """
        dominant_schema_id = max(schema_groups, key=lambda k: len(schema_groups[k]))
        other_schema_ids = [k for k in schema_groups if k != dominant_schema_id]
        logging.info(f'Dominant schema group: "{dominant_schema_id}" with {len(schema_groups[dominant_schema_id])} files.')
        logging.info(f'Other schema groups: {len(other_schema_ids)}')
        return dominant_schema_id, other_schema_ids

    def get_schema_dict_from_identifier(self, schema_id: str) -> Dict[str, str]:
        """
        Converts a schema identifier string back to a schema dictionary.

        Args:
            schema_id (str): Schema identifier string.

        Returns:
            Dict[str, str]: Schema dictionary.
        """
        schema = {}
        for col_dtype in schema_id.split(';'):
            if col_dtype:
                col, dtype = col_dtype.split(':')
                schema[col] = dtype
        return schema

    def align_schema_to_dominant(self, parquet_file: str, dominant_schema: Dict[str, str]) -> bool:
        """
        Attempts to align a Parquet file's schema to the dominant schema.

        Args:
            parquet_file (str): Path to the Parquet file.
            dominant_schema (Dict[str, str]): The dominant schema to conform to.

        Returns:
            bool: True if alignment was successful, False otherwise.
        """
        try:
            df = pd.read_parquet(parquet_file)
            logging.info(f'Reading {parquet_file} for alignment.')

            # Ensure all dominant schema columns are present
            for col in dominant_schema:
                if col not in df.columns:
                    df[col] = None  # Add missing columns with null values

            # Reorder columns to match the dominant schema
            df = df[dominant_schema.keys()]

            # Cast columns to match dominant schema
            for col, dtype in dominant_schema.items():
                try:
                    if 'datetime' in dtype:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    else:
                        df[col] = df[col].astype(dtype, errors='ignore')
                except Exception as e:
                    logging.error(f'Error casting column {col} in file {parquet_file}: {e}')
                    raise e  # Reraise to mark alignment as failed

            # Write the aligned DataFrame back to Parquet
            pq.write_table(pa.Table.from_pandas(df, preserve_index=False), parquet_file)
            logging.info(f'Aligned {parquet_file} to dominant schema.')
            return True
        except Exception as e:
            logging.error(f'Failed to align {parquet_file} to dominant schema: {e}')
            self.failed_files.append(parquet_file)
            return False

    def make_batches(self, files: List[str]) -> List[List[str]]:
        """
        Creates batches of Parquet files based on the maximum batch size.

        Args:
            files (List[str]): List of Parquet file paths with the same schema.

        Returns:
            List[List[str]]: List of batches, each batch is a list of Parquet file paths.
        """
        batches = []
        current_batch = []
        current_size = 0

        for file in files:
            try:
                file_size_mb = os.path.getsize(file) / (1024 * 1024)
                if current_size + file_size_mb > self.max_batch_size_mb and current_batch:
                    batches.append(current_batch)
                    logging.info(f'Created batch with {len(current_batch)} files.')
                    current_batch = [file]
                    current_size = file_size_mb
                else:
                    current_batch.append(file)
                    current_size += file_size_mb
            except Exception as e:
                logging.error(f'Error processing file size for {file}: {e}')
                self.failed_files.append(file)
                continue

        if current_batch:
            batches.append(current_batch)
            logging.info(f'Created batch with {len(current_batch)} files.')

        logging.info(f'Created {len(batches)} total batches.')
        return batches

    def verify_and_filter_files(self, files: List[str]) -> List[str]:
        """
        Verifies the integrity of Parquet files and filters out corrupted ones.

        Args:
            files (List[str]): List of Parquet file paths.

        Returns:
            List[str]: List of valid Parquet file paths.
        """
        valid_files = []
        for file in files:
            if verify_parquet_file(file):
                valid_files.append(file)
            else:
                logging.warning(f'Skipping corrupted file: {file}')
                self.failed_files.append(file)
        return valid_files

    def concat_batch_parquet_files_incremental(self, batch_files: List[str]):
        """
        Concatenates a batch of Parquet files into a single Parquet file incrementally.

        Args:
            batch_files (List[str]): List of Parquet file paths to concatenate.
        """
        # Verify and filter files
        batch_files = self.verify_and_filter_files(batch_files)
        if not batch_files:
            logging.warning('No valid files in the batch to concatenate.')
            return

        try:
            logging.info(f'Starting concatenation of batch with {len(batch_files)} files.')
            tables = [pq.read_table(file) for file in batch_files]
            concatenated_table = pa.concat_tables(tables)
            # Write the concatenated table to the output Parquet file
            # Using append mode if the file already exists
            if os.path.exists(self.output_parquet):
                existing_table = pq.read_table(self.output_parquet)
                concatenated_table = pa.concat_tables([existing_table, concatenated_table])
            pq.write_table(concatenated_table, self.output_parquet)
            logging.info(f'Concatenated batch into {self.output_parquet}.')
        except Exception as e:
            logging.error(f'Error concatenating batch files {batch_files}: {e}')

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

    # Initialize and run Shapefile to Parquet processing
    shapefile_to_parquet_processor = ShapefileToParquet(shp_dir, parquet_dir, log_dir)
    shapefile_list = shapefile_to_parquet_processor.make_list(country_codes=['IT', 'PT', 'GB'])

    for shp_file in shapefile_list:
        shapefile_to_parquet_processor.get_shapefile_columns(shp_file)
        shapefile_to_parquet_processor.convert_to_parquet(shp_file)

    # Create schema DataFrame
    schema_df = shapefile_to_parquet_processor.get_schema_dataframe()
    logging.info('Shapefile processing completed.')

    # Save schema DataFrame for reference (optional)
    schema_output_path = os.path.join(output_dir, 'parquet_schemas.csv')
    schema_df.to_csv(schema_output_path, index=False)
    logging.info(f'Saved schema DataFrame to {schema_output_path}.')

    # Analyze unique schemas
    unique_schema_count = schema_df.drop(columns=['parquet_file']).drop_duplicates().shape[0]
    logging.info(f'Number of unique schemas: {unique_schema_count}')

    # Initialize and run Parquet concatenation
    concatenator = ConcatenateParquet(parquet_dir, output_parquet, max_batch_size_mb=100)

    # List all Parquet files
    all_parquet_files = concatenator.list_parquet_files()
    if not all_parquet_files:
        logging.error('No Parquet files found for concatenation.')
        return

    # Group by schema
    parquet_groups = concatenator.group_parquet_files_by_schema_df(all_parquet_files, schema_df)
    logging.info(f'Found {len(parquet_groups)} unique schema groups.')

    # Identify dominant schema group
    dominant_schema_id, other_schema_ids = concatenator.identify_dominant_schema(parquet_groups)
    dominant_schema = concatenator.get_schema_dict_from_identifier(dominant_schema_id)

    # Process other schema groups to align with dominant schema
    for schema_id in other_schema_ids:
        files_to_align = parquet_groups[schema_id]
        for file in files_to_align:
            success = concatenator.align_schema_to_dominant(file, dominant_schema)
            if not success:
                logging.error(f'Failed to align {file} to dominant schema.')

    # After alignment, re-group the parquet files
    # This should ideally result in only one schema group (dominant)
    # List all Parquet files again
    all_parquet_files = concatenator.list_parquet_files()

    # Create a new schema dataframe after alignment
    # Re-extract schema information
    updated_schema_records = []
    for file in all_parquet_files:
        try:
            df = pd.read_parquet(file)
            schema = {col.lower(): str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
            updated_schema_records.append({
                'parquet_file': os.path.basename(file),
                **schema
            })
        except Exception as e:
            logging.error(f'Error reading schema from {file}: {e}')
            concatenator.failed_files.append(file)

    updated_schema_df = pd.DataFrame(updated_schema_records)
    logging.info('Updated schema DataFrame after alignment.')

    # Save updated schema DataFrame for reference (optional)
    updated_schema_output_path = os.path.join(output_dir, 'parquet_schemas_aligned.csv')
    updated_schema_df.to_csv(updated_schema_output_path, index=False)
    logging.info(f'Saved updated schema DataFrame to {updated_schema_output_path}.')

    # Group parquet files again after alignment
    updated_parquet_groups = concatenator.group_parquet_files_by_schema_df(all_parquet_files, updated_schema_df)
    logging.info(f'After alignment, found {len(updated_parquet_groups)} unique schema groups.')

    if len(updated_parquet_groups) > 1:
        logging.warning('There are still multiple schema groups after alignment. Further processing may be required.')
    elif len(updated_parquet_groups) == 0:
        logging.error('No valid Parquet files available for concatenation after alignment.')
        return

    # Proceed with concatenation only if schemas are aligned
    # Assuming the dominant schema group now includes all compatible files
    for schema_id, files in updated_parquet_groups.items():
        # Assuming all schemas are now aligned to dominant
        batches = concatenator.make_batches(files)
        for batch in batches:
            concatenator.concat_batch_parquet_files_incremental(batch)

    logging.info('All processes completed successfully.')

    # Optionally, handle and report failed files
    if concatenator.failed_files:
        failed_files_path = os.path.join(output_dir, 'failed_parquets.txt')
        with open(failed_files_path, 'w') as f:
            for file in concatenator.failed_files:
                f.write(f"{file}\n")
        logging.info(f'List of failed Parquet files saved to {failed_files_path}.')

if __name__ == '__main__':
    main()
