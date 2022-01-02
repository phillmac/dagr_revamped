import logging
import random
import string
from pprint import pprint

logger = logging.getLogger(__name__)


class Response():
    @staticmethod
    def create(p_title, p_source, headers=None):
        if 'DeviantArt: 500 Internal Server Error' in p_title:
            return Response(content=p_source, headers=headers, status=500)

        if 'DeviantArt: 401 Unauthorized' in p_title or '401 Unauthorized' in p_source:
            return Response(content=p_source, headers=headers, status=401)

        if '404 Not Found' in p_title or 'DeviantArt: 404' in p_title:
            return Response(content=p_source, headers=headers, status=404)

        if '403 ERROR' in p_source:
            return Response(content=p_source, headers=headers, status=403)
        if '504 Gateway Time-out' in p_source:
            return Response(content=p_source, headers=headers, status=504)
        return Response(content=p_source, headers=headers)

    def __init__(self, content='', headers=None, status=200):
        self.__id = ''.join(random.choices(
            string.ascii_uppercase + string.digits, k=5))
        logger.debug('Created Reponse %s', self.__id)
        self.__status = status
        self.__headers = {} if headers is None else headers
        self.__content = content

    def __del__(self):
        logger.debug('Destroying Reponse %s', self.__id)

    @property
    def status_code(self):
        return self.__status

    @property
    def text(self):
        if isinstance(self.__content, str):
            return self.__content
        try:
            if 'ISO-8859-1' in self.headers.get('content-type', ''):
                return self.content.decode('ISO-8859-1')
            return self.content.decode('utf8')
        except UnicodeDecodeError:
            pprint(self.headers)
            raise

    @property
    def headers(self):
        return dict(h.split('\u003a\u0020') for h in self.__headers.split('\u000d\u000a') if len(h.split('\u003a\u0020')) == 2)

    @property
    def content(self):
        if isinstance(self.__content, str):
            return self.__content.encode()
        try:
            return bytearray(self.__content)
        except:
            print(type(self.__content))
            raise
