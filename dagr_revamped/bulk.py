import logging
from docopt import docopt
from datetime import datetime
from pprint import pprint, pformat
from .lib import DAGR
from .config import DAGRConfig
from .version import version
from .dagr_logging import init_logging, log as dagr_log


class DAGRBulkCli():
    """
{} v{}

Usage:
    dagr-bulk.py [options] [-v|-vv|--debug=DEBUGLVL] FILENAMES ...

Options:
    --filter=FILTER                         Filter deviants by name. Comma seperated list.
    -d DIRECTORY --directory=DIRECTORY      Output directory for deviations.
    -p PROGRESS --progress=PROGRESS         Save progress regulary.
    -m --mature                             Rip deviations with the mature content flag set.
    --maxpages=MAXPAGES                     Crawl only a limited number of pages.
    -o --overwrite                          Overwrite already existing deviations present on filesystem.
    -r --reverse                            Rip deviations in reverse order.
    -t --test                               Skip downloading deviations, just print the url instead.
    --nocrawl                               Use only downloaded pages cache instead of crawling deviant's pages.
    --fixartists                            Rebuild artists cache.
    -u --unfindable                         List non-existant albums, collections, galleries, etc.
    -v --verbose                            Show more detail, -vv for debug.
    --verifybest                            Ensure that best quality file is downloded.
    --verifyexists                          Override cache and force check filesystem.
    --fixmissing                            Fix deviations present in cache but missing from filesystem.
    --refreshonly=DATE                      Crawl deviants that have not been crawled since DATE
    --refreshonlydays=DAYS                  Crawl deviants that have not been crawled in DAYS days
    --debug=DEBUGLVL                        Show even more detail.
    --showqueue                             Display inital queue contents. Requires at least -v or --debug=1.
    --useapi                                Use DA API
    --clientid=CLIENTID                     DA API Client ID
    --clientsecret=CLIENTSECRET             DA API Client Secret
    --config_options=CONFIGOPTIONS
    -h --help                               Display this screen.
    --version                               Display version.

"""
    NAME = __package__
    VERSION = version

    def __init__(self, config):
        self.arguments = arguments = docopt(self.__doc__.format(
            self.NAME, self.VERSION), version=self.VERSION)
        ll_arg = None
        try:
            ll_arg = int(arguments.get('--debug')
                         or arguments.get('--verbose'))
        except Exception:
            dagr_log(__name__, logging.WARN, 'Unrecognized debug level')
        self.args = {
            'bulk': True,
            'filenames': arguments.get('FILENAMES'),
            'filter': arguments.get('--filter'),
            'refreshonly': arguments.get('--refreshonly'),
            'refreshonlydays': arguments.get('--refreshonlydays'),
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
            'log_level': ll_arg,
            'showqueue': arguments.get('--showqueue'),
            'useapi': arguments.get('--useapi'),
            'clientid': arguments.get('--clientid'),
            'clientsecret': arguments.get('--clientsecret'),
            'config_options': arguments.get('--config_options')

        }


def main():
    config = DAGRConfig()
    cli = DAGRBulkCli(config)
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
        logging.shutdown()


if __name__ == '__main__':
    main()
