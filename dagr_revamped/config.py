import os
import sys
import json
import logging
from io import StringIO
from pathlib import Path
from copy import deepcopy
from configparser import ConfigParser, NoSectionError
from pprint import pprint, pformat
from .dagr_logging import log as dagr_log


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
        dagr_log(__name__, logging.DEBUG, 'Loaded config files {}'.format(pformat(self.__conf_files)))

    def find_configs(self, ext):
        usr_dir = Path('~/.config/dagr').expanduser()
        locations  = [Path.cwd(), usr_dir] + self.__include
        dagr_log(__name__, 5, 'Looking for configs in {}'.format(locations))
        return [cnf for cnf in
                (search.joinpath(self.__conf_name or 'dagr_settings').with_suffix(ext).resolve() for search in locations)
                    if cnf.exists()]

    def load_ini_config(self):
        if self.__ini_files:
            config = ConfigParser(allow_no_value=True)
            config.read(self.__ini_files)
            return config

    def load_json_config(self):
        settings = {}
        for json_file in self.__json_files:
            with open (json_file, 'r') as fh:
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

    def get(self, section, key=None):
        section = str(section).lower()
        if section in self.__settings:
            if key is None:
                return self.__settings.get(section)
            key = str(key).lower()
            return self.__settings.get(section).get(key)
        raise ValueError('Section {} does not exist'.format(section))

    def set_key(self, section, key, value):
        pass

    def set_section(self, section, value):
        self.__settings[section] = value

    def get_all(self):
        return deepcopy(self.__settings)

    def get_conf_files(self):
        return deepcopy(self.__conf_files)

    def merge_configs(self, section_names, conf_calbacks):
        for sec_name in section_names:
            conf_sections = (cb(sec_name) for cb in conf_calbacks)
            self.__settings[sec_name.lower()] = merge_all(*conf_sections)


class DAGRConfig(DAGRBaseConf):
    DEFAULTS = {
        "Logging": {
            'Format':'%(asctime)s - %(levelname)s - %(message)s'
        },
        'Logging.Extra': {
            4:'TRACE',
            5:'TRACE',
            15:'INFO',
            25:'INFO',
        },
        'Logging.Map': {
            0:logging.WARN,
            1:logging.INFO,
            2: 15,
            3:logging.DEBUG,
            4:5,
            5:4
        },
        'Conf': {
            'Version': '0.1.0'
            },
        'DeviantArt': {
            'BaseUrl': 'https://www.deviantart.com',
            'ArtRegex':(r"https://www\.deviantart\.com/[a-zA-Z0-9_-]*/art/[a-zA-Z0-9_-]*"),
            'MatureContent': False,
            'Modes': 'album,collection,query,scraps,favs,gallery,search,page',
            'MValArgs': 'album,collection,query,category,page',
            'NDModes': 'search',
            'MaxPages': 15000,
            },
        'DeviantArt.FindLink': {
            'FallbackOrder': 'img full,meta,img normal'
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
             'DeviantArt.Modes.Page':{
                'url_fmt': '{base_url}/{deviant_lower}/art/{mval}'
            },
        'DeviantArt.Offsets':{
            'Folder': 10,
            'Page': 24,
            'Search': 10
            },
        'Dagr': {
            'OutputDirectory': '~/dagr',
            'Overwrite': False,
            'Reverse': False,
            #'RecursionLimit': 10000,
            'SaveProgress': 50,
            'Verbose': False,
            },
        'Dagr.SubDirs':{
            'UseOldFormat': False,
            'Move': False
            },
        'Dagr.Cache': {
            'Crawled': '.crawled',
            'Artists': '.artists',
            'FileNames': '.filenames',
            'DownloadedPages': '.dagr_downloaded_pages',
            'Settings': '.settings',
            'Verified':  '.verified',
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
        'Dagr.Plugins.Locations': {
            'Default': '~/.config/dagr/plugins'
            },
        'Dagr.Retry':{
            'SleepDuration': 0.5
            },
        'Dagr.Retry.ExceptionNames': {
            'OSError': True,
            'ChunkedEncodingError': True,
            'ConnectionError': True
            },
        'Dagr.Verify':{
            'DebugLocation':'#Trash/Verify'
            }
    }
    OVERRIDES = {
        'Dagr': {
            'OutputDirectory': Path.cwd()
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
                'MatureContent': 'mature',
                'MaxPages': 'maxpages'
            }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__arguments = None
        self.merge_configs(self.DEFAULTS.keys(), (
            self.get_ini_section,
            self.get_json_section,
            self.OVERRIDES.get,
            self.DEFAULTS.get,
        ))

    def set_args(self, arguments):
        self.__arguments = arguments
        self.merge_configs(self.DEFAULTS.keys(), (
            self.get_args_mapped,
            self.get
        ))
        dagr_log(__name__, 5, 'Config: {}'.format(pformat(self.get_all())))

    def get_args_mapped(self, section):
        if not self.__arguments: return {}
        mapping = self.SETTINGS_MAP.get(section)
        if not mapping: return {}
        return  dict((str(key), self.__arguments.get(mapping.get(key)))
                for key in mapping.keys() if self.__arguments.get(mapping.get(key)))

    def get_modes(self):
        modes = [s.strip() for s in self.get('deviantart', 'modes').split(',')]
        mval_args = [s.strip() for s in self.get('deviantart', 'mvalargs').split(',')]
        return modes, mval_args

    def get_log_level(self):
        if self.get('dagr', 'debug') and self.__arguments.get('log_level') < 3: return 3
        if self.get('dagr', 'verbose') and self.__arguments.get('log_level') < 1: return 1
        return self.__arguments.get('log_level')

    def map_log_level(self):
        return self.get('logging.map', self.get_log_level())

    def conf_cmd(self):
        conf_cmd_maping = {
            'print': self.conf_print,
            'files': self.conf_files
        }
        cmd = self.__arguments.get('conf_cmd')
        if conf_cmd_maping.get(cmd)(): return
        print('Unrecognized {}'.format(cmd))

    def conf_print(self):
        fname = self.__arguments.get('conf_file')
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
            for f in self.get_conf_files():
                if fname == f.name:
                   with f.open('r') as fh:
                    pprint(fh.read())
        return True

    def conf_files(self):
        print('Loaded conf files {}'.format(pformat(self.get_conf_files())))
        return True

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
        dagr_log(__name__, 5, 'Merging {}, {}'.format(result, item))
        result = dict_merge(result, item)
    dagr_log(__name__, 4, 'Result: {}'.format(result))
    return result

def normalize_dict(d):
    return dict((str(k).lower(), convert_val(v)) for k,v in d.items())

def convert_val(val):
    if isinstance(val, str):
        true_vals = ['true', 'yes', 'on', '1']
        false_vals = ['false', 'no', 'off', '0']
        if val.lower() in true_vals: return True
        if val.lower() in false_vals: return False
        try: return int(val)
        except: pass
        try: return float(val)
        except: pass
    return val


class DARGConfigCli():
    """
Usage:
dagr.py config CONF_CMD [CONF_FILE] [-v|-vv|--debug=DEBUGLVL]

Options:

"""
