# src/shapefile_processor/shapefile_processor.py
import os
import logging
from shapefile_parquet_processor.setup_logging import setup_logging
from shapefile_parquet_processor.utils import clean_dir
from shapefile_parquet_processor.shapefile_to_df import shapefile_to_df
from shapefile_parquet_processor.dataframe_corrector import DataFrameCorrector
from shapefile_parquet_processor.parquet_manager import ParquetManager

class ShapefileProcessor:
    """
    Main class to orchestrate shapefile processing and Parquet writing.
    """

    def __init__(self, shp_dir: str, final_parquet_dir: str, log_dir: str):
        """
        Initialize the ShapefileProcessor.

        :param shp_dir: Directory containing shapefiles.
        :param final_parquet_dir: Directory for the final Parquet files.
        :param log_dir: Directory for log files.
        """
        self.shp_dir = shp_dir
        self.final_parquet_dir = final_parquet_dir
        self.log_dir = log_dir
        setup_logging(log_dir)
        self.parquet_manager = ParquetManager(final_parquet_dir)
        self.reference_df = None
        self.df_corrector = None

    def list_shapefiles(self, country_codes: list) -> list:
        """
        List shapefiles filtered by country codes.

        :param country_codes: List of country code prefixes.
        :return: List of shapefile paths.
        """
        try:
            all_files = [file for file in os.listdir(self.shp_dir) if file.endswith('.shp')]
            filtered_files = [file for file in all_files if file[:2] in country_codes]
            logging.info(f'Found {len(filtered_files)} shapefiles to process.')
            return [os.path.join(self.shp_dir, file) for file in filtered_files]
        except Exception as e:
            logging.error(f'Error listing shapefiles: {e}')
            return []

    def process_shapefiles(self, shapefile_paths: list):
        """
        Process all shapefiles and write to Parquet incrementally.

        :param shapefile_paths: List of shapefile paths.
        """
        from shapefile_parquet_processor.shapefile_to_df import shapefile_to_df

        for file_path in shapefile_paths:
            logging.info(f'Processing shapefile: {os.path.basename(file_path)}')
            df = shapefile_to_df(file_path)
            if df is None or df.empty:
                logging.error(f'Failed to process shapefile {file_path}. Skipping.')
                continue

            if self.reference_df is None:
                self.reference_df = df.copy()
                self.df_corrector = DataFrameCorrector(self.reference_df)
                logging.info('Initialized reference DataFrame for data type consistency.')

            else:
                df = self.df_corrector.check_and_correct_dtypes(df)
                if df is None:
                    logging.error(f'Data type correction failed for {file_path}. Skipping.')
                    continue

            self.parquet_manager.add_dataframe(df)

        # Finalize writing any remaining data
        self.parquet_manager.finalize()

    def run(self, country_codes: list = ['IT', 'DK', 'FR', 'GB', 'PT']):
        """
        Execute the shapefile processing workflow.

        :param country_codes: List of country code prefixes for filtering.
        """
        shapefile_paths = self.list_shapefiles(country_codes)
        self.process_shapefiles(shapefile_paths)
        logging.info('Shapefile processing completed.')
