import logging
import sys
from pprint import pformat, pprint

from docopt import docopt

from .config import DAGRConfig
from .dagr_logging import init_logging
from .dagr_logging import log as dagr_log
from .lib import DAGR
from .version import version


class DAGRCli():
    """
{} v{}

Usage:
    dagr.py [-fgs] [options] [-v|-vv|--debug=DEBUGLVL] DEVIANT ...
    dagr.py (-a ALBUM) [options] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (-c COLLECTION) [options] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (-q QUERY) [options] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (-k CATEGORY) [options] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (--page PAGE) [options] [-v|-vv|--debug=DEBUGLVL] DEVIANT
    dagr.py (--search SEARCH) [options] [-v|-vv|--debug=DEBUGLVL]
    dagr.py [options]

Options:
    -a ALBUM --album=ALBUM                  Rip deviations in album
    -c COLLECTION --collection=COLLECTION   Rip deviations in collection
    -f --favs                               Rip deviations in favourites
    -g --gallery                            Rip atrworks in gallery
    -k CATEGORY --category=CATEGORY         Rip gallery with category filter
    --page=PAGE                             Rip a single page
    -q QUERY --query=QUERY                  Rip gallery with query filter
    -s --scraps                             Rip deviations in scraps
    --search=SEARCH                         Search deviantart for results
    --filter=filter                         Filter bulk deviants by name. Comma seperated list
    -d DIRECTORY --directory=DIRECTORY      Output directory for deviations
    -p PROGRESS --progress=PROGRESS         Save progress regulary
    -m --mature                             Rip deviations with the mature content flag set
    --maxpages=MAXPAGES                     Crawl only a limited number of pages
    -o --overwrite                          Overwrite already existing deviations
    -r --reverse                            Rip deviations in reverse order
    -t --test                               Skip downloading deviations, just print the url instead
    --nocrawl                               Use downloaded pages cache instead of crawling deviant's pages
    --fullcrawl                             Ignore crawler cache
    --fixartists                            Rebuild artists cache
    --fixmissing                            Fix deviations present in cache but missing from filesystem.
    --isdeviant                             Skip deviant/group check, force deviant mode
    --isgroup                               Skip deviant/group check, force group mode
    -u --unfindable                         List non-existant albums, collections, galleries, etc
    -v --verbose                            Show more detail, -vv for debug
    --verifybest                            Ensure that best quality file is downloded
    --verifyexists                          Override cache and force check filesystem
    --debug=DEBUGLVL                        Show still more detail
    --quiet                                 Suppress warnings
    --showqueue                             Display inital queue contents. Requires at least -v or --debug=1.
    --useapi                                Use DA API
    --clientid=CLIENTID                     DA API Client ID
    --clientsecret=CLIENTSECRET             DA API Client Secret
    --config_options=CONFIGOPTIONS
    -h --help                               Show this screen
    --version                               Show version

"""
    NAME = __package__
    VERSION = version

    def __init__(self, config):
        self.arguments = arguments = docopt(self.__doc__.format(
            self.NAME, self.VERSION), version=self.VERSION)
        cnf_modes, cnf_mval_args = config.get_modes()
        modes = [m for m in cnf_modes if arguments.get('--'+m)]
        mode_val = next((arguments.get('--'+v)
                         for v in cnf_mval_args if arguments.get('--'+v)), None)
        try:
            ll_arg = -1 if arguments.get('--quiet') else (int(arguments.get('--debug')) if arguments.get(
                '--debug') else (int(arguments.get('--verbose') if arguments.get('--verbose') else 0)))
        except Exception:
            ll_arg = 0
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
            'fullcrawl': arguments.get('--fullcrawl'),
            'nocrawl': arguments.get('--nocrawl'),
            'reverse': arguments.get('--reverse'),
            'test': arguments.get('--test'),
            'isdeviant': arguments.get('--isdeviant'),
            'isgroup': arguments.get('--isgroup'),
            'unfindable': arguments.get('--unfindable'),
            'verifybest': arguments.get('--verifybest'),
            'verifyexists': arguments.get('--verifyexists'),
            'useapi': arguments.get('--useapi'),
            'clientid': arguments.get('--clientid'),
            'clientsecret': arguments.get('--clientsecret'),
            'config_options': arguments.get('--config_options'),
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

    with DAGR(config=config, **cli.args) as ripper:
        ripper.run()
        ripper.print_errors()
        ripper.print_dl_total()


if __name__ == '__main__':
    main()
    logging.shutdown()
