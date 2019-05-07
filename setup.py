#!/usr/bin/env python

from dagr_revamped import __version__
from setuptools import setup, find_packages

setup(
    name='dagr_revamped',
    version=__version__,
    description='A deviantArt Ripper script written in Python',
    author='Phillip Mackintosh',
    url='https://github.com/phillmac/dagr_revamped',
    packages=find_packages(),
    install_requires=[
        'MechanicalSoup >= 0.10.0',
        'docopt == 0.6.2',
        'pluginbase == 1.0.0'
        ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    entry_points={
        'console_scripts': [
            'dagr.py=dagr_revamped.cli:main',
            'dagr-bulk.py=dagr_revamped.bulk:main',
            'dagr-utils.py=dagr_revamped.utils:main',
            'dagr-config.py=dagr_revamped.config:main'
            ]
    }
)
