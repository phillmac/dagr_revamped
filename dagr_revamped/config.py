import json
import logging
import os
from configparser import ConfigParser, NoSectionError
from copy import deepcopy
from pathlib import Path
from pprint import pformat, pprint

from docopt import docopt

from .dagr_logging import init_logging
from .dagr_logging import log as dagr_log
from .version import version


class DAGRBaseConf():

    def __init__(self, *args, **kwargs):
        self.__include = kwargs.get('include') or []
        self.__conf_name = kwargs.get('conf_name')
        self.__settings = {}
        self.__ini_files = self.find_configs('.ini')
        self.__json_files = self.find_configs('.json')
        self.__ini_config = self.load_ini_config()
        self.__json_config = self.load_json_config()
        self.__conf_files = self.__ini_files + self.__json_files
        dagr_log(__name__, logging.DEBUG, 'Loaded config files {}'.format(
            pformat(self.__conf_files)))

    def find_configs(self, ext):
        usr_dir = Path('~/.config/dagr').expanduser()
        locations = [Path.cwd(), usr_dir] + self.__include
        dagr_log(__name__, 5, 'Looking for configs in {}'.format(locations))
        return [cnf for cnf in
                (search.joinpath(self.__conf_name or 'dagr_settings').with_suffix(
                    ext).resolve() for search in locations)
                if cnf.exists()]

    def load_ini_config(self):
        if self.__ini_files:
            config = ConfigParser(allow_no_value=True)
            config.read(self.__ini_files)
            return config

    def load_json_config(self):
        settings = {}
        for json_file in self.__json_files:
            with open(json_file, 'r') as fh:
                settings = dict_merge(settings, json.load(fh))
        return settings

    def get_ini_section(self, section):
        if not self.__ini_config:
            return {}
        try:
            return dict((key, value)
                        for key, value in self.__ini_config.items(section))
        except:
            return {}

    def get_json_section(self, section):
        return deepcopy(self.__json_config.get(section))

    def get(self, section, key=None, key_errors=True):
        section = str(section).lower()
        if section in self.__settings:
            if key is None:
                return self.__settings.get(section)
            key = str(key).lower()
            return convert_val(self.__settings.get(section).get(key))
        if key_errors:
            raise ValueError('Section {} does not exist'.format(section))
        return None

    def set_key(self, section, key, value):
        section = str(section).lower()
        if not section in self.__settings:
            self.__settings[section] = {}
        self.__settings[section][key] = value

    def set_section(self, section, value):
        section = str(section).lower()
        self.__settings[section] = value

    def get_all(self):
        return deepcopy(self.__settings)

    def get_conf_files(self):
        return deepcopy(self.__conf_files)

    def merge_configs(self, section_names, conf_calbacks):
        for sec_name in section_names:
            dagr_log(__name__, 5, f'Merging section {sec_name}')
            conf_sections = (cb(sec_name) for cb in conf_calbacks)
            self.__settings[sec_name.lower()] = merge_all(*conf_sections)


def get_os_options(base_key, keys):
    options = {}
    for k in keys:
        value = os.environ.get(f"{base_key}.{k}".lower(), None)
        if not value is None:
            options[k.lower()] = value
    return options


class DAGRConfig(DAGRBaseConf):
    DEFAULTS = {
        "Logging": {
            'Format': '%(asctime)s - %(levelname)s - %(message)s'
        },
        'Logging.Extra': {
            4: 'TRACE',
            5: 'TRACE',
            15: 'INFO',
            25: 'INFO',
        },
        'Logging.Map': {
            -1: logging.ERROR,
            0: logging.WARN,
            1: logging.INFO,
            2: 15,
            3: logging.DEBUG,
            4: 5,
            5: 4
        },
        'Conf': {
            'Version': '0.1.0'
        },
        'DeviantArt': {
            'BaseUrl': 'https://www.deviantart.com',
            'ArtRegex': (r"https://www\.deviantart\.com/[a-zA-Z0-9_-]*/art/[a-zA-Z0-9_-]*"),
            'MatureContent': False,
            'Antisocial': True,
            'Modes': 'album,collection,query,scraps,favs,gallery,search,page',
            'MValArgs': 'album,collection,query,category,page,search',
            'NDModes': 'search',
            'MaxPages': 15000
        },
        'DeviantArt.Modes.Album': {
            'url_fmt': '{base_url}/{deviant_lower}/gallery/{mval}?offset={offset}'
        },
        'DeviantArt.Modes.Category': {
            'url_fmt': '{base_url}/{deviant_lower}/gallery/?catpath={mval}&offset={offset}'
        },
        'DeviantArt.Modes.Collection': {
            'url_fmt': '{base_url}/{deviant_lower}/favourites/{mval}?offset={offset}'
        },
        'DeviantArt.Modes.Query': {
            'url_fmt': '{base_url}/{deviant_lower}/gallery/?q={mval}&offset={offset}'
        },
        'DeviantArt.Modes.Scraps': {
            'url_fmt': '{base_url}/{deviant_lower}/gallery/?catpath=scraps&offset={offset}'
        },
        'DeviantArt.Modes.Favs': {
            'url_fmt': '{base_url}/{deviant_lower}/favourites/?catpath=/&offset={offset}',
            'group_url_fmt': '{base_url}/{deviant_lower}/favourites/?offset={offset}',
            'folder_regex': 'class="ch-top" href="{base_url}/{deviant_lower}/favourites/([0-9]*/[a-zA-Z0-9_-]*)"',
            'folder_url_fmt': '{base_url}/{deviant_lower}/favourites/{mval}/?offset={offset}'
        },
        'DeviantArt.Modes.Gallery': {
            'url_fmt': '{base_url}/{deviant_lower}/gallery/?catpath=/&offset={offset}',
            'group_url_fmt': '{base_url}/{deviant_lower}/gallery?offset={offset}',
            'folder_regex': 'class="ch-top" href="{base_url}/{deviant_lower}/gallery/([0-9]*/[a-zA-Z0-9_-]*)"',
            'folder_url_fmt': '{base_url}/{deviant_lower}/gallery/{mval}/?offset={offset}'
        },
        'DeviantArt.Modes.Search': {
            'url_fmt': '{base_url}?q={mval}&offset={offset}'
        },
        'DeviantArt.Modes.Page': {
            'url_fmt': '{base_url}/{deviant_lower}/art/{mval}'
        },
        'DeviantArt.Offsets': {
            'Folder': 10,
            'Page': 24,
            'Search': 10
        },
        'Dagr': {
            'OutputDirectory': '~/dagr',
            'Overwrite': False,
            'Reverse': False,
            # 'RecursionLimit': 10000,
            'SaveProgress': 50,
            'DownloadDelay': 0.300,
            'Verbose': False,
        },
        'Dagr.SubDirs': {
            'UseOldFormat': False,
            'Move': False
        },
        'Dagr.Cache': {
            'Crawled': '.crawled',
            'Artists': '.artists',
            'FileNames': '.filenames',
            'DownloadedPages': '.dagr_downloaded_pages',
            'NoLink': '.nolink',
            'Settings': '.settings',
            'Verified':  '.verified',
            'Queue': '.queue',
            'Premium': '.premium',
            'ShortUrls': False
        },
        'Dagr.BS4.Config': {
            'Features': 'lxml'
        },
        'Dagr.MimeTypes': {
            'image/vnd.adobe.photoshop': '.psd',
            'image/photoshop': '.psd',
            'application/rar': '.rar',
            'application/x-rar-compressed': '.rar',
            'application/x-rar': '.rar',
            'image/x-canon-cr2': '.tif',
            'application/x-7z-compressed': '.7z',
            'application/x-lha': '.lzh',
            'application/zip': '.zip',
            'image/x-ms-bmp': '.bmp',
            'application/x-shockwave-flash': '.swf'
        },
        'Dagr.Plugins': {
            'Disabled': ''
        },
        'Dagr.Plugins.Classes': {
            'Browser': 'Default',
            'Ripper': 'Default',
            'Crawler': 'Default',
            'Processor': 'Default'
        },
        'Dagr.Plugins.Locations': {
            'Default': '~/.config/dagr/plugins'
        },
        'Dagr.Plugins.Selenium': {
            'CachePath': '.selenium'
        },
        'Dagr.Retry': {
            'SleepDuration': 0.5
        },
        'Dagr.Retry.ExceptionNames': {
            'OSError': True,
            'ChunkedEncodingError': True,
            'ConnectionError': True
        },
        'Dagr.Verify': {
            'DebugLocation': ''
        },
        'Dagr.FindLink': {
            'DebugLocation': '',
            'FallbackOrder': 'img full,meta,img normal'
        },
    }
    OVERRIDES = {
        'Dagr': {
            'OutputDirectory': Path.cwd()
        },
        'Dagr.Plugins.Classes': get_os_options('Dagr.Plugins.Classes', ['Browser', 'Ripper', 'Resolver', 'Crawler', 'Processor']),
        'Dagr.Plugins.Selenium': get_os_options('Dagr.Plugins.Selenium', ['Enabled', 'Webdriver_mode', 'Webdriver_url', 'Driver_path']),
        'Deviantart': get_os_options('Deviantart', ['Username', 'Password'])
    }
    SETTINGS_MAP = {
        'Dagr': {
            'OutputDirectory': 'directory',
            'Overwrite': 'overwrite',
            'SaveProgress': 'progress',
            'Verbose': 'verbose',
            'Reverse': 'reverse'
        },
        'DeviantArt': {
            'MatureContent': 'mature',
            'MaxPages': 'maxpages',
        }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__arguments = None
        self.__config_options = {}
        self.merge_configs(self.DEFAULTS.keys(), (
            self.get_ini_section,
            self.get_json_section,
            self.OVERRIDES.get,
            self.DEFAULTS.get,
        ))

    def set_args(self, arguments):
        self.__arguments = arguments
        if not self.__arguments.get('config_options') is None:
            for opt in self.__arguments.get('config_options').split(','):
                [opt_name, opt_value] = opt.lower().split(':')
                if (not opt_name) or (not opt_value):
                    dagr_log(__name__, logging.WARNING,
                             f'Unable to parse config option {opt_name} : {opt_value}')
                    continue
                opt_name_parts = opt_name.split('.')
                opt_name_parts.reverse()
                opt_key, *opt_sec_parts = opt_name_parts
                opt_sec_parts.reverse()
                opt_section = '.'.join(opt_sec_parts)
                self.__config_options = dict_merge(
                    self.__config_options, {opt_section: {opt_key: opt_value}})

            dagr_log(__name__, 30,
                     f'Config Options: {pformat(self.__config_options)}')

        self.merge_configs(set([*self.DEFAULTS.keys(),  *self.__config_options.keys()]), (
            self.get_args_mapped,
            self.__get_config_options,
            lambda section: self.get(section, key_errors=False),

        ))
        dagr_log(__name__, 5, 'Config: {}'.format(pformat(self.get_all())))

    def get_args_mapped(self, section):
        if not self.__arguments:
            return {}
        mapping = self.SETTINGS_MAP.get(section)
        if not mapping:
            return {}
        return dict((str(key), self.__arguments.get(mapping.get(key)))
                    for key in mapping.keys() if self.__arguments.get(mapping.get(key)))

    def __get_config_options(self, section):
        return self.__config_options.get(section.lower())

    def get_modes(self):
        modes = [s.strip() for s in self.get('deviantart', 'modes').split(',')]
        mval_args = [s.strip() for s in self.get(
            'deviantart', 'mvalargs').split(',')]
        return modes, mval_args

    def get_log_level(self):
        if self.get('dagr', 'debug') and self.__arguments.get('log_level') < 3:
            return 3
        if self.get('dagr', 'verbose') and self.__arguments.get('log_level') < 1:
            return 1
        return self.__arguments.get('log_level')

    def map_log_level(self):
        return self.get('logging.map', self.get_log_level())

    def conf_cmd(self):
        conf_cmd_maping = {
            None: self.action_missing,
            'files': self.conf_files,
            'print': self.conf_print,
            'get': self.show_config,
            'set': self.set_config
        }
        cmd = self.__arguments.get('conf_cmd')
        mapped = conf_cmd_maping.get(cmd)
        if mapped and mapped():
            return
        print(f"Unrecognized command {cmd}")

    def action_missing(self):
        print('CONF_CMD is required')
        return True

    def show_config(self):
        section = self.__arguments.get('section')
        if not section:
            pprint(self.get_all())
        else:
            key = self.__arguments.get('key', None)
            conf_val = self.get(section, key)
            if not key:
                pprint({f"{section}": conf_val})
                return True
            pprint({f"{section}.{key}": conf_val})
        return True

    def conf_print(self):
        fname = self.__arguments.get('conf_file')
        if not fname:
            pprint(self.get_all())
        elif fname == '.ini':
            for f in self.__ini_files:
                with open(f, 'r') as fh:
                    pprint(fh.read())
        elif fname == '.json':
            for f in self.__json_files:
                with open(f, 'r') as fh:
                    pprint(fh.read())
        else:
            for f in self.get_conf_files():
                if fname == f.name:
                    with f.open('r') as fh:
                        pprint(fh.read())
        return True

    def conf_files(self):
        print('Loaded conf files:')
        pprint([str(f) for f in self.get_conf_files()])
        return True

    def set_config(self):
        if not self.__arguments['section']:
            print('--section is required')


def dict_merge(dict_1, dict_2):
    """Merge two dictionaries.
    Values that evaluate to true take priority over falsy values.
    `dict_1` takes priority over `dict_2`.
    """
    dict_1 = normalize_dict(dict_1)
    dict_2 = normalize_dict(dict_2)
    return dict((key, dict_1.get(key) or dict_2.get(key))
                for key in set(dict_2) | set(dict_1))


def merge_all(*dicts):
    result = {}
    for item in filter(lambda d: d, dicts):
        dagr_log(__name__, 5, f'Merging {result}, {item}')
        result = dict_merge(result, item)
    dagr_log(__name__, 4, 'Result: {}'.format(result))
    return result


def normalize_dict(d):
    return dict((str(k).lower(), convert_val(v)) for k, v in d.items())


def convert_val(val):
    if isinstance(val, str):
        true_vals = ['true', 'yes', 'on', '1']
        false_vals = ['false', 'no', 'off', '0']
        if val.lower() in true_vals:
            return True
        if val.lower() in false_vals:
            return False
        try:
            return int(val)
        except:
            pass
        try:
            return float(val)
        except:
            pass
    return val


class DARGConfigCli():
    """
{} v{}
Usage:
dagr-config.py CONF_CMD [options] [CONF_FILE] [-v|-vv|--debug=DEBUGLVL]

Options:
    -k KEY --key=KEY                  Get or set value of config key
    -s SECTION --section=SECTION      Get or set value of config section

"""

    NAME = __package__
    VERSION = version

    def __init__(self, config):
        self.config = DAGRConfig()
        self.arguments = arguments = docopt(self.__doc__.format(
            self.NAME, self.VERSION), version=self.VERSION)

        try:
            ll_arg = -1 if arguments.get('--quiet') else (int(arguments.get('--debug')) if arguments.get(
                '--debug') else (int(arguments.get('--verbose') if arguments.get('--verbose') else 0)))
        except Exception:
            ll_arg = 0
            dagr_log(__name__, logging.WARN, 'Unrecognized debug level')

        self.args = {
            'conf_cmd': arguments.get('CONF_CMD', None),
            'key': arguments.get('--key'),
            'section': arguments.get('--section'),
            'log_level': ll_arg
        }


def main():
    config = DAGRConfig()
    cli = DARGConfigCli(config)
    config.set_args(cli.args)
    init_logging(config)
    logger = logging.getLogger(__name__)
    logger.log(level=5, msg=pformat(cli.arguments))
    logger.debug(pformat(cli.args))
    config.conf_cmd()

    if __name__ == '__main__':
        logging.shutdown()


if __name__ == '__main__':
    main()
