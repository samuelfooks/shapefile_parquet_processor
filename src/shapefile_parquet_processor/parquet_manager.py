# src/shapefile_processor/parquet_manager.py
import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class ParquetManager:
    """
    Manages incremental writing of DataFrames to Parquet files with partitioning and indexing.

    Attributes:
        output_dir (str): Directory to store Parquet files.
        partition_size (int): Number of rows per Parquet partition.
        compression (str): Compression codec for Parquet.
    """
    def __init__(self, output_dir: str, partition_size: int = 500000, compression: str = 'snappy'):
        """
        Initialize the ParquetManager.

        :param output_dir: Directory to store Parquet files.
        :param partition_size: Number of rows per Parquet partition.
        :param compression: Compression codec for Parquet.
        """
        self.output_dir = output_dir
        self.partition_size = partition_size
        self.compression = compression
        self.current_batch = []
        self.current_size = 0
        os.makedirs(self.output_dir, exist_ok=True)
        self.parquet_files = []

    def add_dataframe(self, df: pd.DataFrame):
        """
        Add a DataFrame to the current batch.

        :param df: DataFrame to add.
        """
        self.current_batch.append(df)
        self.current_size += len(df)
        logging.info(f'Added DataFrame with {len(df)} rows to current batch. Total batch size: {self.current_size}')
        if self.current_size >= self.partition_size:
            self.write_batch()

    def write_batch(self):
        """
        Write the current batch to a Parquet file and reset the batch.
        """
        if not self.current_batch:
            return
        try:
            batch_df = pd.concat(self.current_batch, ignore_index=True)
            table = pa.Table.from_pandas(batch_df)
            parquet_filename = os.path.join(
                self.output_dir,
                f'part-{len(self.parquet_files):05d}.parquet'
            )
            pq.write_table(table, parquet_filename, compression=self.compression)
            self.parquet_files.append(parquet_filename)
            logging.info(f'Wrote batch to {parquet_filename}')
            # Reset batch
            self.current_batch = []
            self.current_size = 0
        except Exception as e:
            logging.error(f'Error writing batch to Parquet: {e}')

    def finalize(self):
        """
        Write any remaining data in the current batch to Parquet.
        """
        if self.current_batch:
            self.write_batch()
        logging.info(f'Finalized writing. Total Parquet files: {len(self.parquet_files)}')
