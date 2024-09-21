# src/shapefile_processor/df_corrector.py
import logging
import pandas as pd

class DataFrameCorrector:
    """
    A class to handle data type consistency across DataFrames.
    """

    def __init__(self, reference_df: pd.DataFrame):
        """
        Initialize with a reference DataFrame to match data types.

        :param reference_df: Reference DataFrame.
        """
        self.reference_df = reference_df

    def check_and_correct_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure that the DataFrame has the same data types as the reference DataFrame.

        :param df: DataFrame to check and correct.
        :return: DataFrame with corrected data types or None if error occurs.
        """
        try:
            for col in self.reference_df.columns:
                if col in df.columns:
                    if df[col].dtype != self.reference_df[col].dtype:
                        df[col] = df[col].astype(self.reference_df[col].dtype)
                        logging.info(f'Corrected dtype of column {col} to {self.reference_df[col].dtype}')
                else:
                    # If column is missing, add it with NaNs
                    df[col] = pd.NA
                    logging.warning(f'Added missing column {col} with NaNs.')
            # Drop any extra columns not in reference
            extra_cols = set(df.columns) - set(self.reference_df.columns)
            if extra_cols:
                df = df.drop(columns=extra_cols)
                logging.warning(f'Dropped extra columns: {extra_cols}')
            return df
        except Exception as e:
            logging.error(f'Error correcting data types: {e}')
            return None
