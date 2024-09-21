# src/shapefile_processor/shapefile_to_df.py
import logging
import pandas as pd
from shapefile_to_gdf import shapefile_to_gdf

def shapefile_to_df(file_path: str, crs: str = 'EPSG:4326') -> pd.DataFrame:
    """
    Process a shapefile and convert it to a DataFrame with WKT geometries.

    :param file_path: Path to the shapefile.
    :param crs: Desired Coordinate Reference System.
    :return: Processed DataFrame or None if error occurs.
    """
    gdf = shapefile_to_gdf(file_path, crs)
    if gdf is None:
        return None

    try:
        invalid_geoms = ~gdf.is_valid
        if invalid_geoms.any():
            logging.warning(f'Found {invalid_geoms.sum()} invalid geometries. Attempting to fix.')
            gdf.loc[invalid_geoms, 'geometry'] = gdf.loc[invalid_geoms, 'geometry'].buffer(0)
            # Recheck validity
            still_invalid = ~gdf.is_valid
            if still_invalid.any():
                logging.error(f'{still_invalid.sum()} geometries could not be fixed and will be dropped.')
                gdf = gdf[~still_invalid].reset_index(drop=True)

        gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt if geom else None)
        df = pd.DataFrame(gdf)

        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]) and df[col].isnull().any():
                df[col] = df[col].astype('float64')
                logging.info(f'Converted column {col} from int to float due to NaNs.')

        logging.info(f'Successfully processed shapefile {os.path.basename(file_path)}')
        return df
    except Exception as e:
        logging.error(f'Error processing shapefile {file_path}: {e}')
        return None
