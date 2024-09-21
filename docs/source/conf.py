# docs/conf.py
import os
import sys
import sphinx_rtd_theme
sys.path.insert(0, os.path.abspath('../../src'))

project = 'shapefile_processor'
author = 'Samuel Fooks'
release = '0.1.0'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',  # Supports NumPy and Google style docstrings
    'sphinx.ext.viewcode',
    'sphinx_rtd_theme',     # If using Read the Docs theme
]
templates_path = ['_templates']
exclude_patterns = []

html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
html_static_path = ['_static']
