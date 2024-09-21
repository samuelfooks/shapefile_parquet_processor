# setup.py
from setuptools import setup, find_packages

setup(
    name='shapefile_parquet_processor',
    version='0.1.0',
    description='A module for processing shapefiles into DataFrames and Parquet files.',
    author='Samuel Fooks',
    author_email='samuel.fooks@gmail.com',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pandas',
        'geopandas',
        'dask',
        'dask-expr',
        'dask-geopandas',
        'cartopy',
        'fiona',
        'matplotlib',
        'pyarrow',
        'pyogrio',
        # Add other dependencies as needed
    ],
    entry_points={
        'console_scripts': [
            'shp_parprocessor=shapefile_parquet_processor.main:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3.9',
        'Operating System :: OS Independent',
    ],
)
