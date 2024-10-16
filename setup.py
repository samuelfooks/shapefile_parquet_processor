# setup.py
from setuptools import setup, find_packages

setup(
    name='vector_to_parquet_processor',
    version='0.1.0',
    description='A module for processing geospatial vector files into Parquet and Geoparquet',
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
            'shape_geoparquet_processor=shapefiles_to_parquet:shapefiles_to_geoparquet.py',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3.9',
        'Operating System :: OS Independent',
    ],
)
