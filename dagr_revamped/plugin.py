import os
import logging
from copy import deepcopy
from pluginbase import PluginBase

here = os.path.abspath(os.path.dirname(__file__))

class PluginManager():
    logger = logging.getLogger(__name__)
    def __init__(self, app):
        self.__app = app
        self.__locations = [v for k, v in app.config.get('dagr.plugins.locations').items()]
        self.__config = app.config.get('dagr.plugins')
        self.__disabled = self.__config.get('disabled') or []
        self.__funcs = {}
        plugin_base = PluginBase(package='{}.plugins'.format(__package__),
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
                    plugin.setup(self)
                except DagrImportError:
                    logging.warning('Unable to import plugin {}'.format(plugin_name), exc_info=True)

    def __register(self, cat, name, func):
        if not cat in self.__funcs:
            self.__funcs[cat] = {}
        self.__funcs[cat][name] = func

    def get_funcs(self, cat):
        return deepcopy(list(self.__funcs.get(cat, []).items()))

    def register_findlink(self, name, func):
        self.__register('findlink', name, func)

    def register_findlink_b(self, name, func):
        self.__register('findlink_b', name, func)

class DagrImportError(Exception):
    pass
