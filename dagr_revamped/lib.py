import re
import json
import logging
import threading
import portalocker
import collections.abc
from os import getcwd, makedirs, rename, utime, remove as os_remove
from os.path import (
    abspath, basename, dirname, exists as path_exists,
    expanduser, join as path_join, sep as path_sep
    )
from requests import (
    adapters as req_adapters,
    codes as req_codes,
    session as req_session
    )
from random import choice
from bs4.element import Tag
from mechanicalsoup import StatefulBrowser

class DAGR():
    SETTINGS = DAGRConfig()
    def __init__(self, **kwargs):
        self.__work_queue = {}
        self.bulk = kwargs.get('bulk')
        self.modes = kwargs.get('modes')
        self.mode_val = kwargs.get('mode_val')
        self.deviants = kwargs.get('deviants')
        self.filenames = kwargs.get('filenames')
        self.directory = kwargs.get('directory') or getcwd()
        self.mature = kwargs.get('mature')
        self.overwrite = kwargs.get('overwrite')
        self.progress = kwargs.get('progress')
        self.test = kwargs.get('test')
        self.browser = None
        self.stop_running = threading.Event()
        if self.deviants or self.bulk and self.filenames:
            self.__work_queue = self.__build_queue()

    @staticmethod
    def _update_d(d, u):
        for k, v in u.items():
            if isinstance(v,  collections.abc.Mapping):
                d[k] = DAGR._update_d(d.get(k, {}), v)
            elif isinstance(d.get(k), collections.abc.Iterable):
                if isinstance(v, collections.abc.Iterable):
                    d[k].extend(v)
                else:
                    d[k].append(v)
            else:
                d[k] = v
        return d

    def get_queue(self):
        return self.__work_queue

    def __build_queue(self):
        logger = logging.getLogger(__name__)
        if self.bulk:
            bulk_queue = {}
            self.filenames = [abspath(fn) for fn in self.filenames]
            for fn in self.filenames:
                logger.debug('Loading file {}'.format(fn))
                with open(fn, 'r') as fh:
                    DAGR._update_d(bulk_queue,json.load(fh))
            wq = DAGR._convert_queue(bulk_queue)
        else:
            wq ={}
        logger.log(level=5, msg=json.dumps(wq, indent=4, sort_keys=True))
        return wq

    @staticmethod
    def _convert_queue(queue):
        logger = logging.getLogger(__name__)
        converted = queue.get('deviants', {})
        if 'search' in queue:
            search = queue.pop('search')
        for mode in DAGR.MODES.keys():
            data = queue.get(mode)
            if isinstance(data, collections.abc.Mapping):
                for k, v in data.items():
                    DAGR._update_d(converted, {k:{mode:v}})
            elif isinstance(data, collections.abc.Iterable):
                for v in data:
                    DAGR._update_d(converted, {v:{mode:None}})
            else:
                logger.debug('Mode {} not present'.format(mode))
        return converted


    def save_queue(self, path='.queue'):
        with open(path, 'w') as fh:
            json.dump(self.work_queue, fh)

    def load_queue(self, path='.queue'):
        with open(path, 'r') as fh:
            self.work_queue = json.load(fh)

    def queue_add(self, work):
        return DAGR.__update_d(self.get_queue(), work)

    def keep_running(self):
        return not self.stop_running.is_set()

    def run(self):
        logger = logging.getLogger(__name__)
        if not self.get_queue():
            raise ValueError('Empty work queue')
        wq = self.get_queue()
        self.browser_init()
        while self.keep_running():
            try:
                deviant = next(iter(wq))
                modes =  wq.pop(deviant)
            except StopIteration:
                break
            try:
                deviant, group = self.get_deviant(deviant)
            except DagrException:
                logger.warn('Deviant {} not found or deactivated!'.fromat(deviant))
                continue
            logger.log(level=5, msg='Running {} : {}'.format(deviant, modes))
            try:
                self.make_dirs(path_join(self.directory, deviant))
            except OSError:
                logger.exception('Failed to create deviant diectory {}'.format(deviant), level=logging.WARN)
                continue
            self.rip(deviant, modes, group)

    def rip(self, deviant, modes, group=False):
        logger = logging.getLogger(__name__)
        for mode, mode_vals in modes.items():
            if mode_vals:
                for mval in mode_vals:
                    logger.info('Ripping {} : {} : {}'.format(deviant, mode, mval))
                    self._rip(mode, deviant, mval)
            else:
                logger.info('Ripping {} : {}'.format(deviant, mode))
                self._rip(mode, deviant)

    def _rip(self, mode, deviant=None, mval=None):
        logger=logging.getLogger(__name__)
        base_dir = self.get_base_dir(mode, deviant, mval)
        if not base_dir: return
        try:
            with portalocker.TemporaryFileLock(path_join(base_dir, '.lock'),
                    fail_when_locked = True): pass
        except (portalocker.exceptions.LockException,portalocker.exceptions.AlreadyLocked):
            logger.warn('Skipping locked directory {}'.format(base_dir))
            return


    def browser_init(self):
        if not self.browser:
            # Set up fake browser
            self._set_browser()

    def _set_browser(self):
        user_agents = (
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1'
            ' (KHTML, like Gecko) Chrome/14.0.835.202 Safari/535.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:7.0.1) Gecko/20100101',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/534.50'
            ' (KHTML, like Gecko) Version/5.1 Safari/534.50',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
            'Opera/9.99 (Windows NT 5.1; U; pl) Presto/9.9.9',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_6; en-US)'
            ' AppleWebKit/530.5 (KHTML, like Gecko) Chrome/ Safari/530.5',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.2'
            ' (KHTML, like Gecko) Chrome/6.0',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; pl; rv:1.9.1)'
            ' Gecko/20090624 Firefox/3.5 (.NET CLR 3.5.30729)'
        )
        session = req_session()
        session.headers.update({'Referer': 'https://www.deviantart.com/'})
        if self.mature:
            session.cookies.update({'agegate_state': '1'})
        session.mount('https://', req_adapters.HTTPAdapter(max_retries=3))

        self.browser = StatefulBrowser(session=session,
                                       user_agent=choice(user_agents))

    def get_deviant(self, deviant):
        group = False
        html = self.get('https://www.deviantart.com/{}/'.format(deviant)).text
        search = re.search(r'<title>.[A-Za-z0-9-]*', html,
                            re.IGNORECASE).group(0)[7:]
        deviant = re.sub('[^a-zA-Z0-9_-]+', '', search)
        if re.search('<dt class="f h">Group</dt>', html):
            group = True
        return deviant, group

    def get(self, url):
        logger = logging.getLogger(__name__)
        tries = {}
        response = None
        while True:
            try:
                response =  self.get_response(url)
                break
            except Exception as ex:
                logger.exception('Get exception', level=logging.DEBUG)
                except_name = type(ex).__name__
                if except_name in self.RETRY_EXCEPTION_NAMES:
                    if not except_name in tries:
                        tries[except_name] = 0
                    tries[except_name] += 1
                    if tries[except_name]  < 3:
                        continue
                    raise DagrException('Failed to get url: {}'.format(except_name))
                else:
                    raise

        if not response.status_code == req_codes.ok:
            raise DagrException('incorrect status code - {}'.format(response.status_code))
        return response

    def get_response(self, url, *args, **kwargs):
        if isinstance(url, Tag):
            if hasattr(url, 'attrs') and 'href' in url.attrs:
                url = self.browser.absolute_url(url['href'])
        return self.browser.session.get(url, timeout=150)
    @staticmethod
    def make_dirs(directory):
        logger = logging.getLogger(__name__)
        if not path_exists(directory):
            makedirs(directory)
            logger.debug('Created dir {}'.format(directory))

    def get_base_dir(self, mode, deviant=None, mval=None):
        logger = logging.getLogger(__name__)
        if deviant:
            base_dir = path_join(self.directory, deviant, mode)
        else:
            base_dir =  path_join(self.directory, mode)
        if mval:
            base_dir = path_join(base_dir, mval)
            base_dir = abspath(base_dir)
        try:
            DAGR.make_dirs(base_dir)
        except OSError:
            logger.exception('Unable to create base_dir', level=logging.ERROR)
            return
        return base_dir

class DagrException(Exception):
    def __init__(self, value):
        super(DagrException, self).__init__(value)
        self.parameter = value

    def __str__(self):
        return str(self.parameter)