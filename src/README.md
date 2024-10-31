# shapefiles_to_parquet

Processes many shapefiles into separate parquet files and then makes a partioned geoparquet file structure

The shapefiles should contain the same column names.

You can select shapefiles by putting in a string(s) into a list.  The script will look for these strings in the filenames and add these files to the list to process


    Process shapefiles:
    Convert all selected shapefiles to parquet.  Change the datatypes to types suited for dask dataframes, and append all the parquet files to ensure compatible datatypes.  The geometries are converted to WKT for dask dataframes to handle.
    If only a subset of columns are shared between all shapefiles, include these in the 'columns_to_convert' list in the config.json

    Concatenate parquet files:
    Concatenate all the parquet files together into chunks of a determined size.  200-400 MB is optimal.  With optimal partitioning the parquet is performant.  The geometries are in WKT format, so need to be read into a geodataframe for Geo relevant use.

    Convert into geoparquet:
    This step maps each of the partitions into a geoparquet format with the geometry as WKB.  It then tries to repartition all the partitions into 1 partition, but depending on the size of the data, will fail
    

    In config.json

    Set your shapefile input directory.  The script will find all files ending with '.shp'.  Use file name selectors if you need to

    You can choose which step you want to do if you already have completed previous steps and just want to test, or run a specific step.

    {
        "input_dir": "../data/surveymaps",
        "output_dir": "../data/parquet",
        "log_dir": "../data/logs",
        "file_name_selectors": ["IT"],
        "columns_to_convert" : ["hab_type", "orig_hab", "sum_conf", "t_relate", "comp"]

    }



    tests

    test_shp_geopq_conversion.py

    Takes a control column as input, must be a column with a value(s) unique to the shapefiles
    Takes 3 random shapefiles and 5 geometries from each and looks for them in the converted geoparquet partitions in '../data/parquet/geoparquet/'.  Then cross checks that the matched geometries from the parquet file have the same value in the control column as the value of the control column from the shapefile.  

    thematiclots_attributes_csvs
    
    arco_variables.ipynb  -  A notebook used to explore/refine csvs that have the lists of the data products that need to be combined.  Can be used to find for example all the columns that are shared between the files.