import os
import logging
from copy import deepcopy
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
        return deepcopy(self.__app.config)

    def __register(self, cat, name, func):
        if not cat in self.__funcs:
            self.__funcs[cat] = {}
        self.__funcs[cat][name] = func

    def get_funcs(self, cat):
        return deepcopy(self.__funcs.get(cat, {}))

    def register_findlink(self, name, func):
        self.__register('findlink', name, func)

    def register_findlink_b(self, name, func):
        self.__register('findlink_b', name, func)

    def register_browser(self, name, func):
        self.__register('browser', name, func)


class DagrImportError(Exception):
    pass


class DagrPluginConfigError(Exception):
    pass
