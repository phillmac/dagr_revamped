import sys
import logging
from docopt import docopt
from pprint import pprint, pformat
from .lib import DAGR
from .config import DAGRConfig
from .version import version
from .dagr_logging import init_logging, log as dagr_log

class DAGRCli():
    """
{} v{}

Usage:
    dagr.py bulk [-mrotu --nocrawl --verifybest --verifyexists --fixmissing --fixartists --maxpages=MAXPAGES -d DIRECTORY -p PROGRESS --filter=FILTER] [--isdeviant | --isgroup] [-v|-vv|--debug=DEBUGLVL] FILENAMES ...
    dagr.py [-fgs] [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT ...
    dagr.py (-a ALBUM) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (-c COLLECTION) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (-q QUERY) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (-k CATEGORY) [-mrot -d DIRECTORY -p PROGRESS] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (--page PAGE) [-mrot -d DIRECTORY -p PROGRESS] [--isdeviant | --isgroup] [-v|-vv|--debug=DEBUGLVL] DEVIANT

Options:
    -a ALBUM --album=ALBUM                  Rip deviations in album
    -c COLLECTION --collection=COLLECTION   Rip deviations in collection
    -f --favs                               Rip deviations in favourites
    -g --gallery                            Rip atrworks in gallery
    -k CATEGORY --category=CATEGORY         Rip gallery with category filter
    --page=PAGE                             Rip a single page
    -q QUERY --query=QUERY                  Rip gallery with query filter
    -s --scraps                             Rip deviations in scraps
    --filter=filter                         Filter bulk deviants by name. Comma seperated list
    -d DIRECTORY --directory=DIRECTORY      Output directory for deviations
    -p PROGRESS --progress=PROGRESS         Save progress regulary
    -m --mature                             Rip deviations with the mature content flag set
    --maxpages=MAXPAGES                     Crawl only a limited number of pages
    -o --overwrite                          Overwrite already existing deviations
    -r --reverse                            Rip deviations in reverse order
    -t --test                               Skip downloading deviations, just print the url instead
    --nocrawl                               Use downloaded pages cache instead of crawling deviant's pages
    --fixartists                            Rebuild artists cache
    --isdeviant                             Skip deviant/group check, force deviant mode
    --isgroup                               Skip deviant/group check, force group mode
    -u --unfindable                         List non-existant albums, collections, galleries, etc
    -v --verbose                            Show more detail, -vv for debug
    --verifybest                            Ensure that best quality file is downloded
    --verifyexists                          Override cache and force check filesystem
    --debug=DEBUGLVL                        Show still more detail
    -h --help                               Show this screen.
    --version                               Show version.

"""
    NAME = __package__
    VERSION = version

    def __init__(self, config):
        self.arguments = arguments = docopt(self.__doc__.format(self.NAME, self.VERSION), version=self.VERSION)
        cnf_modes, cnf_mval_args = config.get_modes()
        modes = [m for m in cnf_modes if arguments.get('--'+m)]
        mode_val = next((arguments.get('--'+v) for v in cnf_mval_args if arguments.get('--'+v)), None)
        try:
            ll_arg = int(arguments.get('--debug') or arguments.get('--verbose'))
        except Exception:
            dagr_log(__name__, logging.WARN, 'Unrecognized debug level')
        self.args = {
            'modes': modes, 'mode_val': mode_val,
            'bulk': arguments.get('bulk'),
            'deviants': arguments.get('DEVIANT'),
            'filenames': arguments.get('FILENAMES'),
            'filter': arguments.get('--filter'),
            'directory': arguments.get('--directory'),
            'mature': arguments.get('--mature'),
            'maxpages': arguments.get('--maxpages'),
            'overwrite': arguments.get('--overwrite'),
            'progress': arguments.get('--progress'),
            'fixartists': arguments.get('--fixartists'),
            'fixmissing': arguments.get('--fixmissing'),
            'nocrawl': arguments.get('--nocrawl'),
            'reverse': arguments.get('--reverse'),
            'test': arguments.get('--test'),
            'isdeviant': arguments.get('--isdeviant'),
            'isgroup': arguments.get('--isgroup'),
            'unfindable': arguments.get('--unfindable'),
            'verifybest': arguments.get('--verifybest'),
            'verifyexists': arguments.get('--verifyexists'),
            'conf_cmd': arguments.get('CONF_CMD'),
            'conf_file': arguments.get('CONF_FILE'),
            'log_level': ll_arg
        }


def main():
    config = DAGRConfig()
    cli = DAGRCli(config)
    config.set_args(cli.args)
    init_logging(config)
    logger = logging.getLogger(__name__)
    logger.log(level=5, msg=pformat(cli.arguments))
    logger.debug(pformat(cli.args))
    if cli.args.get('conf_cmd'):
        config.conf_cmd()
    else:
        ripper = DAGR(config=config, **cli.args)
        ripper.run()
        ripper.print_errors()
    if __name__ == '__main__':
        logging.shutdown()


if __name__ == '__main__':
    main()
