#!/usr/bin/env python

from setuptools import setup, find_packages

version = None
exec(open('dagr_revamped/version.py').read())
with open('README.md', 'r') as fh:
    long_description = fh.read()
setup(
    name='dagr_revamped',
    version=version,
    description='A deviantArt Ripper script written in Python',
    author='Phillip Mackintosh',
    url='https://github.com/phillmac/dagr_revamped',
    packages=find_packages(),
    package_data={'dagr_revamped': ['builtin_plugins/*']},
    install_requires=[
        'MechanicalSoup >= 0.10.0',
        'docopt == 0.6.2',
        'pluginbase == 1.0.0',
        'portalocker == 1.4.0',
        'python-dateutil == 2.7.5',
        'deviantart @ git+https://github.com/phillmac/deviantart@c21ce195b466618ae16a9146412b3b713be71ef5'
    ],
    extras_require={
        'calmjs':  ["calmjs==3.3.1"],
        'selenium': ['selenium==3.141.0']
    },
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
