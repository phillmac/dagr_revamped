import os
import json
import platform
import logging
from copy import deepcopy
from configparser import ConfigParser, NoSectionError
from io import StringIO
from pprint import pprint, pformat
from os.path import (
    abspath,
    basename,
    expanduser,
    exists as path_exists,
    join as path_join,
)

class DAGRConfig():
    DEFAULTS = {
        'Conf': {
            'Version': '0.1.0'
        },
        'DeviantArt': {
            'BaseUrl': 'https://www.deviantart.com',
            'ArtRegex':(r"https://www\.deviantart\.com/[a-zA-Z0-9_-]*/art/[a-zA-Z0-9_-]*"),
            'MatureContent': False,
            'Modes': 'album,collection,query,scraps,favs,gallery,search',
            'MaxPages': 10000
        },
        'DeviantArt.Modes.Album':{
            'url_fmt': '{base_url}/{deviant_lower}/gallery/{mval}?offset={offset}'
            },
            'DeviantArt.Modes.Category':{
                'url_fmt': '{base_url}/{deviant_lower}/gallery/?catpath={mval}&offset={offset}'
            },
            'DeviantArt.Modes.Collection':{
                'url_fmt': '{base_url}/{deviant_lower}/favourites/{mval}?offset={offset}'
            },
            'DeviantArt.Modes.Query':{
                'url_fmt': '{base_url}/{deviant_lower}/gallery/?q={mval}&offset={offset}'
            },
            'DeviantArt.Modes.Scraps':{
                'url_fmt': '{base_url}/{deviant_lower}/gallery/?catpath=scraps&offset={offset}'
            },
            'DeviantArt.Modes.Favs':{
                'url_fmt': '{base_url}/{deviant_lower}/favourites/?catpath=/&offset={offset}',
                'group_url_fmt': '{base_url}/{deviant_lower}/favourites/?offset={offset}',
                'folder_regex': 'class="ch-top" href="{base_url}/{deviant_lower}/favourites/([0-9]*/[a-zA-Z0-9_-]*)"',
                'folder_url_fmt': '{base_url}/{deviant_lower}/favourites/{mval}/?offset={offset}'
            },
            'DeviantArt.Modes.Gallery':{
                'url_fmt': '{base_url}/{deviant_lower}/gallery/?catpath=/&offset={offset}',
                'group_url_fmt': '{base_url}/{deviant_lower}/gallery?offset={offset}',
                'folder_regex': 'class="ch-top" href="{base_url}/{deviant_lower}/gallery/([0-9]*/[a-zA-Z0-9_-]*)"',
                'folder_url_fmt': '{base_url}/{deviant_lower}/gallery/{mval}/?offset={offset}'
            },
            'DeviantArt.Modes.Search':{
                'url_fmt': '{base_url}?q={mval}&offset={offset}'
            },
        'DeviantArt.Offsets':{
            'Folder': 10,
            'Page': 24,
            'Search': 10
        },
        'Dagr': {
            'OutputDirectory': '~/dagr',
            'Overwrite': False,
            'SaveProgress': 50,
            'Verbose': False,
            'Reverse': False,
            'RecursionLimit': 10000
        },
        'Dagr.SubDirs':{
            'UseOldFormat': False,
            'Move': False
        },
        'Dagr.Cache': {
            'Artists': '.artists',
            'FileNames': '.filenames',
            'DownloadedPages': '.dagr_downloaded_pages'
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
        'Dagr.Plugins.Locations': {
            'Default': '~/.config/dagr/plugins'
        },
        'Dagr.RetryExceptionNames': {
            'OSError': True,
            'ChunkedEncodingError': True,
            'ConnectionError': True
        }
    }
    OVERRIDES = {
        'Dagr': {
            'OutputDirectory': os.getcwd()
        }
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
                'MatureContent': 'mature'
            }
    }
    def load_ini_config(self):
        ini_files = DAGRConfig.find_configs('.ini')
        self.__ini_files = ini_files
        if ini_files:
            config = ConfigParser(allow_no_value=True)
            config.read(ini_files)
            return config

    def load_json_config(self):
        json_files = DAGRConfig.find_configs('.json')
        settings = {}
        for json_file in json_files:
            self.__json_files.append(json_file)
            with open (json_file, 'r') as fh:
                settings = DAGRConfig.merge(settings, json.load(fh))
        return settings

    @staticmethod
    def find_configs(ext):
        logger =logging.getLogger(__name__)
        usr_dir = expanduser('~/.config/dagr')
        cwd = os.getcwd()
        locations  = [cwd, usr_dir]
        logger.log(level=5, msg='Looking for configs in {}'.format(locations))
        return [cf_path for cf_path in
                (abspath(path_join(search, 'dagr_settings'+ ext)) for search in locations)
                    if path_exists(cf_path)]

    @staticmethod
    def merge(dict_1, dict_2):
        """Merge two dictionaries.
        Values that evaluate to true take priority over falsy values.
        `dict_1` takes priority over `dict_2`.
        """
        return dict((str(key).lower(), dict_1.get(key) or dict_1.get(key.lower()) or dict_2.get(key) or dict_2.get(key.lower()))
                    for key in set(dict_2) | set(dict_1))

    @staticmethod
    def merge_all(*dicts):
        logger = logging.getLogger(__name__)
        result = {}
        for item in filter(lambda d: d, dicts):
            logger.log(level=5, msg=('Merging {}, {}'.format(result, item)))
            result = DAGRConfig.merge(result, item)
        logger.log(level=4, msg=('Result: {}'.format(result, item)))
        return result

    @staticmethod
    def get_args_mapped(arguments, section):
        mapping = DAGRConfig.SETTINGS_MAP.get(section)
        if not mapping: return {}
        return  dict((str(key), arguments.get(mapping.get(key)))
                for key in mapping.keys() if arguments.get(mapping.get(key)))

    def __init__(self, arguments):
        logger = logging.getLogger(__name__)
        self.arguments = arguments
        self.__settings = {}
        self.__ini_files = []
        self.__json_files = []
        self.__ini_config = self.load_ini_config()
        self.__json_config = self.load_json_config()
        self.__conf_files = self.__ini_files + self.__json_files
        for section in self.DEFAULTS.keys():
            defaults = self.DEFAULTS.get(section)
            overrides = self.OVERRIDES.get(section)
            json_section =  self.__json_config.get(section)
            ini_section = self.get_ini_section(section)
            args_mapped = self.get_args_mapped(arguments, section)
            self.__settings.update({section.lower():
                self.merge_all(
                    args_mapped,
                    json_section,
                    ini_section,
                    overrides,
                    defaults)})
        logger.debug('Loaded config files {}'.format(pformat(self.__conf_files)))
        logger.log(level=5, msg='Config: {}'.format(pformat(self.__settings)))

    def get(self, section, value=None):
        true_vals = ['true', 'yes', 'on', '1']
        false_vals = ['false', 'no', 'off', '0']
        section = section.lower()
        if section in self.__settings:
            if value is None:
                return self.__settings.get(section)
            value = value.lower()
            val = self.__settings.get(section).get(value)
            if isinstance(val, str):
                if val.lower() in true_vals: return True
                if val.lower() in false_vals: return False
                try: return int(val)
                except: pass
                try: return float(val)
                except: pass
            return val
        raise ValueError('Section {} does not exist'.format(section))

    def get_all(self):
        return deepcopy(self.__settings)

    def get_ini_section(self, section):
        if not self.__ini_config:
            return {}
        try:
            return dict((key, value)
                    for key, value in self.__ini_config.items(section))
        except NoSectionError:
            return {}

    def conf_cmd(self):
        conf_cmd_maping = {
            'print': self.conf_print,
            'files': self.conf_files
        }
        cmd = self.arguments.get('conf_cmd')
        if conf_cmd_maping.get(cmd)(): return
        print('Unrecognized {}'.format(cmd))

    def conf_print(self):
        fname = self.arguments.get('conf_file')
        if not fname:
            pprint(self.__settings)
        elif fname == '.ini':
            for f in self.__ini_files:
                with open(f, 'r') as fh:
                    pprint(fh.read())
        elif fname == '.json':
            for f in self.__json_files:
                with open(f, 'r') as fh:
                    pprint(fh.read())
        else:
            for f in self.__conf_files:
                if fname == basename(f):
                   with open(f, 'r') as fh:
                    pprint(fh.read())
        return True

    def conf_files(self):
        print('Loaded conf files {}'.format(pformat(self.__conf_files)))
        return True


class DagrDockerConfig():
    REQUIRED = [
        'MYSQL_CONN'
    ]

    OPTIONAL = [
    ]

    def __init__(self):
        self.mysql_conn = None
        for req in self.REQUIRED:
            val = os.getenv(req)
            if val is None: raise ValueError('Environment var {} must be set'.format(req))
            self.__dict__.update({req.lower(), val})
        for opt in self.OPTIONAL:
            val = os.getenv(req)
            if val is None: continue
            self.__dict__.update({opt.lower(), val})
