#!/usr/bin/env python

import re
import dagr_revamped
from setuptools import setup, find_packages

__version__ = dagr_revamped.__version__

setup(
    name='dagr_revamped',
    version=__version__,
    description='A deviantArt image downloader script written in Python',
    author='Phillip Mackintosh',
    url='https://github.com/phillmac/dagr',
    data_files=[('share/dagr', ['dagr_settings.ini.sample'])],
    packages=find_packages(),
    install_requires=["MechanicalSoup >= 0.10.0"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        'console_scripts': ['dagr.py=dagr_revamped.cli:main']
    }
)
