import sys
import logging
from docopt import docopt
from pprint import pprint, pformat
from . import __version__
from .lib import DAGR
from .config import DAGRConfig

class DAGRCli():
    """
{} v{}

Usage:
    dagr.py bulk [-mrotu -d DIRECTORY -p PROGRESS --filter=FILTER] [-v|-vv|--debug=DEBUGLVL] FILENAMES ...
    dagr.py config CONF_CMD [CONF_FILE] [-v|-vv|--debug=DEBUGLVL]
    dagr.py [-fgs] [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT ...
    dagr.py (-a ALBUM) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (-c COLLECTION) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (-q QUERY) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (-k CATEGORY) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT

Options:
    -a ALBUM --album=ALBUM                  Rip deviations in album
    -c COLLECTION --collection=COLLECTION   Rip deviations in collection
    -q QUERY --query=QUERY                  Rip gallery with query filter
    -k CATEGORY --category=CATEGORY         Rip gallery with category filter
    -f --favs                               Rip deviations in favourites
    --filter=filter                         Filter bulk deviants by name. Comma seperated list
    -g --gallery                            Rip atrworks in gallery
    -s --scraps                             Rip deviations in scraps
    -d DIRECTORY --directory=DIRECTORY      Output directory for deviations
    -p PROGRESS --progress=PROGRESS         Save progress regulary
    -m --mature                             Rip deviations with the mature content flag set
    -o --overwrite                          Overwrite already existing deviations
    -r --reverse                            Rip deviations in reverse order
    -t --test                               Skip downloading deviations, just print the url instead
    -u --unfindable                         List non-existant albums, collections, galleries, etc
    -v --verbose                            Show more detail, -vv for debug
    --debug=DEBUGLVL                        Show still more detail
    -h --help                               Show this screen.
    --version                               Show version.

"""
    NAME = __package__
    VERSION = __version__

    def __init__(self):
        self.warnings = []
        self.arguments = arguments = docopt(self.__doc__.format(self.NAME, self.VERSION), version=self.VERSION)
        mode_val_args = ['--album', '--collection','--query', '--category']
        modes = [m for m in DAGR.MODES if arguments.get('--'+m)]
        if arguments.get('--unfindable'): modes.append('unfindable')
        mode_val = next((arguments.get(v) for v in mode_val_args if arguments.get(v)), None)
        ll_map = {0:logging.WARN, 1:logging.INFO, 2: 15, 3:logging.DEBUG, 4:5, 5:4}
        try:
            ll_arg = int(arguments.get('--debug') or arguments.get('--verbose'))
        except Exception:
            self.warnings.append('Unrecognized debug level')
        log_level = ll_map.get(ll_arg, logging.WARN)
        self.args = {
            'modes': modes, 'mode_val': mode_val,
            'bulk': arguments.get('bulk'),
            'deviants': arguments.get('DEVIANT'),
            'filenames': arguments.get('FILENAMES'),
            'filter': arguments.get('--filter'),
            'directory': arguments.get('--directory'),
            'mature': arguments.get('--mature'),
            'overwrite': arguments.get('--overwrite'),
            'progress': arguments.get('--progress'),
            'reverse': arguments.get('--reverse'),
            'test': arguments.get('--test'),
            'config': arguments.get('config'),
            'conf_cmd': arguments.get('CONF_CMD'),
            'conf_file': arguments.get('CONF_FILE'),
            'log_level': log_level
        }


def main():
    logfmt = '%(asctime)s - %(levelname)s - %(message)s'
    cli = DAGRCli()
    logging.basicConfig(format=logfmt, stream=sys.stdout, level=cli.args.get('log_level'))
    logger = logging.getLogger(__name__)
    logger.log(level=5, msg=pformat(cli.arguments))
    logger.debug(pformat(cli.args))
    for warning in cli.warnings:
        logger.warn(warning)
    if cli.args.get('config'):
        config = DAGRConfig(cli.args)
        config.conf_cmd()
    else:
        ripper = DAGR(**cli.args)
        ripper.run()
        ripper.print_errors()
    if __name__ == '__main__':
        logging.shutdown()


if __name__ == '__main__':
    main()
