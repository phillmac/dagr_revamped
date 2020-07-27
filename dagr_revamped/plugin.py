import logging
import os
from sys import version_info
from copy import copy, deepcopy
from pathlib import Path

from pluginbase import PluginBase

here = os.path.abspath(os.path.dirname(__file__))


class PluginManager():
    logger = logging.getLogger(__name__)

    def __init__(self, app):
        self.__app = app
        self.__locations = [v for k, v in app.config.get(
            'dagr.plugins.locations').items()]
        self.__config = app.config.get('dagr.plugins')
        self.__disabled = self.__config.get('disabled') or []
        self.__disabled.append('classes')
        self.__funcs = {}
        self.__loaded_plugins = {}
        plugin_base = PluginBase(package=f'{__package__}.plugins',
                                 searchpath=[os.path.join(here, 'builtin_plugins')])
        if self.__locations:
            self.source = plugin_base.make_plugin_source(
                searchpath=self.__locations,
                identifier=__package__)
            for plugin_name in (
                pn for pn in self.source.list_plugins()
                    if not pn in self.__disabled):
                try:
                    plugin = self.source.load_plugin(plugin_name)
                    enabled = plugin.setup(self)
                    self.__loaded_plugins[plugin_name] = {
                        'enabled': enabled
                    }
                except DagrImportError:
                    logging.warning(
                        f'Unable to import plugin {plugin_name}', exc_info=True)
                except DagrPluginConfigError:
                    logging.warning(
                        f'Unable to initialise plugin {plugin_name}', exc_info=True)

    @property
    def loaded_plugins(self):
        return [k for k in self.__loaded_plugins.keys()]

    @property
    def enabled_plugins(self):
        return [k for k, v in self.__loaded_plugins.items() if v['enabled']]

    @property
    def config(self):
        return deepcopy(self.__config)

    @property
    def app_config(self):
        if version_info < (3, 7):
            return copy(self.__app.config)
        return deepcopy(self.__app.config)

    @property
    def output_dir(self):
        return Path(self.__app.config.output_dir)

    def __register(self, cat, name, func):
        if not cat in self.__funcs:
            self.__funcs[cat] = {}
        self.__funcs[cat][name] = func

    def get_funcs(self, cat):
        return {k: v for k, v in self.__funcs.get(cat, {}).items()}

    def register_findlink(self, name, func):
        self.__register('findlink', name, func)

    def register_findlink_b(self, name, func):
        self.__register('findlink_b', name, func)

    def register_browser(self, name, func):
        self.__register('browser', name, func)

    def register_crawler(self, name, func):
        self.__register('crawler', name, func)


class DagrImportError(Exception):
    pass


class DagrPluginConfigError(Exception):
    pass


class DagrPluginDisabledError(Exception):
    pass
