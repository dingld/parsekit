import re
import six
import inspect
import logging
from w3lib.url import urljoin, to_native_str
from scrapy import Item
from scrapy.loader.processors import Identity
from scrapy.loader.processors import TakeFirst
from scrapy.loader.processors import Join


logger = logging.getLogger(__name__)


def recursive(obj, func=lambda x: x):
    if isinstance(obj, (dict, Item)):
        klass = obj.__class__
        d = {k: recursive(v, func) for k, v in obj.items()}
        return klass(**d)

    elif isinstance(obj, (list, tuple, set)):
        klass = obj.__class__
        return klass(recursive(x, func) for x in obj if x is not None)
    else:
        if isinstance(obj, six.text_type):
            obj = to_native_str(obj)
        return func(obj)


class Strip:

    def __init__(self, pattern='\r\t\n '):
        self.func = lambda x: x.strip(pattern)

    def __call__(self, values):
        return recursive(values, func=self.func)


class PJoin():
    name = 'pjoin' # join paragraph

    def __init__(self, separator=u'\n\n'):
        self.separator = separator


class UrlJoin:

    def __call__(self, url, loader_context=None):
        if loader_context:
            response=loader_context.get('response')
            return recursive(url, response.urljoin)
        return url


PROCS = {
    'int': lambda x: recursive(x,int),
    'float': lambda x: recursive(x, float),
    'identity': Identity(),
    'takefirst': TakeFirst(),
    'join': Join(),
    'urljoin': UrlJoin(),
    'pjoin': Join(separator='\n\n'),
    'strip': Strip()
}

def register(key, func):
    if callable(func):
        PROCS[key] = func