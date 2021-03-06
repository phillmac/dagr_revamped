from pprint import pprint


class Response():
    def __init__(self, content='', headers={}, status=200):
        self.__status = status
        self.__headers = headers
        self.__content = content

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
