import sys
import json
import logging
from docopt import docopt
from . import __version__
from .lib import DAGR

class DAGRCli():
    docstring = """
DaGR.

Usage:
    dagr.py bulk [-mrot -d DIRECTORY -p PROGRESS] [-v | -vv | --debug] FILENAMES ...
    dagr.py [-fgs] [-mrot -d DIRECTORY -p PROGRESS] [-v | -vv | --debug] DEVIANT ...
    dagr.py (-a ALBUM) [-mrot -d DIRECTORY -p PROGRESS] [-v | -vv | --debug] DEVIANT
    dagr.py (-c COLLECTION) [-mrot -d DIRECTORY -p PROGRESS] [-v | -vv | --debug] DEVIANT
    dagr.py (-q QUERY) [-mrot -d DIRECTORY -p PROGRESS] [-v | -vv | --debug] DEVIANT
    dagr.py (-k CATEGORY) [-mrot -d DIRECTORY -p PROGRESS] [-v | -vv | --debug] DEVIANT

Options:
    -a ALBUM --album=ALBUM                  Rip artworks in album
    -c COLLECTION --collection=COLLECTION   Rip artworks in collection
    -q QUERY --query=QUERY                  Rip gallery with query filter
    -k CATEGORY --category=CATEGORY         Rip gallery with category filter
    -f --favs                               Rip artworks in favourites
    -g --gallery                            Rip atrworks in gallery
    -s --scraps                             Rip artworks in scraps
    -d DIRECTORY --directory=DIRECTORY      Output directory for artworks
    -p PROGRESS --progress=PROGRESS         Save progress regulary
    -m --mature                             Rip artworks with the mature content flag set
    -o --overwrite                          Overwrite already existing artworks
    -r --reverse                            Rip artworks in reverse order
    -t --test                               Skip downloading artwork, just print the url instead
    -v --verbose                            Show more detail, -vv for debug
    --debug                                 Show still more detail
    -h --help                               Show this screen.
    --version                               Show version.

"""

    def __init__(self):
        arguments = docopt(self.docstring, version=__version__)
        mode_args = ['album', 'collection', 'query', 'category', 'favs', 'scraps', 'gallery']
        mode_val_args = ['--album', '--collection','--query', '--category']
        modes = [m for m in mode_args if arguments.get('--'+m)]
        mode_val = next((arguments.get(v) for v in mode_val_args if arguments.get(v)), None)
        log_level = None
        if arguments.get('--debug') or arguments.get('--verbose') > 1:
            log_level = logging.DEBUG
        elif arguments.get('--verbose'):
            log_level = logging.INFO
        else:
            log_level=logging.WARN
        self.args = {
            'modes': modes, 'mode_val': mode_val,
            'bulk': arguments.get('bulk'),
            'deviants': arguments.get('DEVIANT'),
            'filenames': arguments.get('FILENAMES'),
            'directory': arguments.get('--directory'),
            'mature': arguments.get('--mature'),
            'overwrite': arguments.get('--overwrite'),
            'progress': arguments.get('--progress'),
            'test': arguments.get('--test'),
            'log_level': log_level
        }


def main():
    logfmt = '%(asctime)s - %(levelname)s - %(message)s'
    cli_args = DAGRCli().args
    logging.basicConfig(format=logfmt, stream=sys.stdout, level=cli_args.get('log_level'))
    logger = logging.getLogger(__name__)
    logger.debug(json.dumps(cli_args, indent=4, sort_keys=True))
    ripper = DAGR(**cli_args)
    ripper.run()
    if __name__ == '__main__':
        logging.shutdown()


if __name__ == '__main__':
    main()
