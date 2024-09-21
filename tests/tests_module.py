
# tests/test_module.py
import unittest
from shapefile_parquet_processor.utils import clean_dir
from shapefile_parquet_processor.shapefile_to_gdf import shapefile_to_gdf
from shapefile_parquet_processor.shapefile_to_df import shapefile_to_df
from shapefile_parquet_processor.dataframe_corrector import DataFrameCorrector
from shapefile_parquet_processor.parquet_manager import ParquetManager
import geopandas as gpd
import pandas as pd
import os
import shutil

class TestShapefileProcessor(unittest.TestCase):

    def test_clean_dir(self):
        test_dir = 'test_logs'
        clean_dir(test_dir)
        self.assertTrue(os.path.exists(test_dir))
        # Clean up
        shutil.rmtree(test_dir)

    def test_shapefile_to_gdf(self):
        # Provide a sample shapefile path
        sample_shp = 'path/to/sample.shp'
        gdf = shapefile_to_gdf(sample_shp)
        self.assertIsNotNone(gdf)
        self.assertIsInstance(gdf, gpd.GeoDataFrame)

    def test_shapefile_to_df(self):
        # Provide a sample shapefile path
        sample_shp = 'path/to/sample.shp'
        df = shapefile_to_df(sample_shp)
        self.assertIsNotNone(df)
        self.assertIsInstance(df, pd.DataFrame)

    def test_df_corrector(self):
        ref_df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10.0, 20.0, 30.0]
        })
        target_df = pd.DataFrame({
            'id': [4, 5, 6],
            'value': [40, 50, 60]
        })
        corrector = DataFrameCorrector(ref_df)
        corrected_df = corrector.check_and_correct_dtypes(target_df)
        self.assertEqual(corrected_df['value'].dtype, ref_df['value'].dtype)

    def test_parquet_manager(self):
        output_dir = 'test_parquet'
        manager = ParquetManager(output_dir, partition_size=2)
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
        manager.add_dataframe(df1)
        manager.add_dataframe(df2)
        manager.finalize()
        self.assertTrue(len(manager.parquet_files) == 1)
        # Clean up
        shutil.rmtree(output_dir)

if __name__ == '__main__':
    unittest.main()
