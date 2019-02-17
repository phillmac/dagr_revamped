import sys
import json
import logging
from docopt import docopt
from . import __version__
from .lib import DAGR

class DAGRCli():
    NAME = __file__
    VERSION = __version__
    docstring = """
{} v{}

Usage:
    dagr.py bulk [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|-vvv|--debug] FILENAMES ...
    dagr.py [-fgs] [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|-vvv|--debug] DEVIANT ...
    dagr.py (-a ALBUM) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|-vvv|--debug] DEVIANT
    dagr.py (-c COLLECTION) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|-vvv|--debug] DEVIANT
    dagr.py (-q QUERY) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|-vvv|--debug] DEVIANT
    dagr.py (-k CATEGORY) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|-vvv|--debug] DEVIANT

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
    --debug                                 Show still more detail, same as -vv
    -h --help                               Show this screen.
    --version                               Show version.

"""

    def __init__(self):
        arguments = docopt(self.docstring.format(DAGRCli.NAME, DAGRCli.VERSION), version=DAGRCli.VERSION)
        mode_val_args = ['--album', '--collection','--query', '--category']
        modes = [m for m in DAGR.MODES.keys() if arguments.get('--'+m)]
        mode_val = next((arguments.get(v) for v in mode_val_args if arguments.get(v)), None)
        log_level = None
        ll_map = {0: logging.WARN, 1: logging.INFO, 2:logging.DEBUG, 3:5}
        if arguments.get('--debug'):
            log_level = logging.DEBUG
        else:
            log_level = ll_map.get(arguments.get('--verbose'), logging.WARN)
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
