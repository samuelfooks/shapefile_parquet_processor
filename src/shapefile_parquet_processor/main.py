# src/shapefile_processor/main.py
import os
from shapefile_parquet_processor.utils import clean_dir
from shapefile_parquet_processor.shapefile_processor import ShapefileProcessor

def main():
    """
    Main function to execute the shapefile processing.
    """
    wkdir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(wkdir, '../data/logs')
    clean_dir(log_dir)

    shp_dir = os.path.join(wkdir, '../data/surveymaps')
    final_parquet_dir = os.path.join(wkdir, '../data/sbh_survey_parquet')

    # Clean final parquet directory
    clean_dir(final_parquet_dir)

    # Initialize and run the processor
    processor = ShapefileProcessor(shp_dir, final_parquet_dir, log_dir)
    processor.run()

if __name__ == '__main__':
    main()
