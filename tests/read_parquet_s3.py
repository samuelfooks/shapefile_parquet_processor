import urllib
import s3fs
import dask_geopandas as dgpd
import dask.dataframe as dd
import time

# to be used once parquet files are available in the S3 bucket, still in development
class ParquetReader:
    def __init__(self, source):
        self.source = source
        self.df = None

    def read_parquet(self):
        if self.source.startswith('http') or self.source.startswith('s3'):
            self.df = self.read_parquet_from_s3(self.source)
        else:
            self.df = self.read_parquet_from_local(self.source)
        return self.df

    def read_parquet_from_s3(self, download_url):
        parsed_url = urllib.parse.urlparse(download_url)
        net_loc = parsed_url.netloc
        object_key = parsed_url.path.lstrip('/')
        s3_uri = f's3://{object_key}'
        endpoint_url = f'https://{net_loc}'
        fs = s3fs.S3FileSystem(anon=True, use_ssl=True, client_kwargs={'endpoint_url': endpoint_url})
        df = dd.read_parquet(
            s3_uri,
            storage_options={'anon': True, 'client_kwargs': {'endpoint_url': endpoint_url}},
            spatial_partitions=36  # Adjust this to match the number of spatial partitions in your file
        )
        return df

    def read_parquet_from_local(self, file_path):
        df = dd.read_parquet(file_path, spatial_partitions=36)  # Adjust this to match the number of spatial partitions in your file
        return df


class DataProcessor:
    def __init__(self, df):
        self.df = df

    def print_data_types(self):
        print("Data types:")
        print(self.df.dtypes)

    def drop_geo_columns(self):
        start_time = time.time()
        self.df = self.df.drop(columns=['geometry'])
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'Dropped geometry column in {elapsed_time:.2f} seconds')
        print(self.df.columns, self.df.dtypes)

    def get_top_aphiaids(self, n=5):
        start_time = time.time()
        top_aphiaids = self.df['aphiaid'].value_counts().head(n)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'Computed top {n} aphiaids in {elapsed_time:.2f} seconds')
        return top_aphiaids

    def extract_lats_lons(self, aphiaid):
        start_time = time.time()
        try:
            lats_lons = self.df[self.df['aphiaid'] == int(aphiaid)][['latitude', 'longitude']].compute()
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f'Extracted latitude and longitude values in {elapsed_time:.2f} seconds')
            return lats_lons
        except UnicodeDecodeError as e:
            print(f"Error extracting latitude and longitude values: {e}")
            return None

    def print_sample_rows(self, aphiaid):
        start_time = time.time()
        try:
            # Ensure the operation is performed on a Dask DataFrame
            sample_rows = self.df[self.df['aphiaid'] == int(aphiaid)].head(5)
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f'Computed sample rows in {elapsed_time:.2f} seconds')
            print(sample_rows)
        except UnicodeDecodeError as e:
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Error reading sample rows in {elapsed_time:.2f} seconds: {e}")


# Example usage
parquet_url = 'https://s3.waw3-1.cloudferro.com/emodnet/biology/eurobis_occurrence_data/eurobis_occurrences_geoparquet_2024-10-01.parquet'
# parquet_url = 'data/temp_assets/eurobis_occurrences_geoparquet2024-09-05.parquet'  # Uncomment this line to use a local file

start_time = time.time()

# Read the parquet file
reader = ParquetReader(parquet_url)
df = reader.read_parquet()

# Process the data
processor = DataProcessor(df)
processor.print_data_types()

processor.drop_geo_columns()

top_5_aphiaids = processor.get_top_aphiaids()
print(f'Top 5 most common aphiaid values:\n{top_5_aphiaids}')

third_most_common_aphiaid = top_5_aphiaids.index[2]
print(f"Sample rows for aphiaid {third_most_common_aphiaid}:")
processor.print_sample_rows(third_most_common_aphiaid)

lat_lon_values = processor.extract_lats_lons(third_most_common_aphiaid)
if lat_lon_values is not None:
    print(f'Latitude and Longitude values for the 3rd most common aphiaid ({third_most_common_aphiaid}):\n{lat_lon_values}')

end_time = time.time()
elapsed_time = end_time - start_time
print(f'Total elapsed time: {elapsed_time:.2f} seconds')