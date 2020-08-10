from dagr_revamped.lib import DAGR


class DAGRManager():
    def __init__(self, config):
        self.__config = config
        self.__dagr = None
        self.__cache = None

    def get_browser(self):
        return self.get_dagr().browser

    def get_crawler(self):
        return self.get_dagr().create_crawler()

    def get_dagr(self) -> DAGR:
        if self.__dagr is None:
            self.__dagr = DAGR(
                config=self.__config)
        return self.__dagr

    def get_cache(self):
        if not self.__cache:
            self.__cache = self.get_dagr().pl_manager.get_funcs(
                'crawler_cache').get('selenium')()
        return self.__cache
