import os, re
import time, datetime
import logging
from weakref import WeakKeyDictionary
from copy import deepcopy
from w3lib.url import urljoin
from scrapy import Request, Field
from scrapy.utils.misc import arg_to_iter, flatten
from scrapy.loader import ItemLoader as _ItemLoader, Item
from scrapy.loader.processors import TakeFirst, Identity
from scrapy.exceptions import NotConfigured
from parsekit.processors import PROCS
from parsekit.loader import ItemLoader
from parsekit.schema import (validate, load_parser, 
                            compile_fields, filter_outout)


_ITEM_REFS = WeakKeyDictionary()
logger = logging.getLogger(__name__)


class Template:

    _cache = {}
    def __init__(self, template, name):
        self.name = name
        self.template = template
        validate(template)
        self._compiled = False
        self.initialize()
        self.compile()
    
    @property
    def items(self):
        return self.template['items']
    
    @property
    def links(self):
        return self.template['links']
    
    def initialize(self):
        self.template.setdefault('items', [])
        self.template.setdefault('links', [])

    def compile(self):
        if self._compiled:
            return
        for option in self.items:
            self._compile(option)
        for option in self.links:
            self._compile(option, links=True)
        self._compiled = True
    
    def _compile(self, option, links=False):
        fields = option.get('fields', [])
        compile_fields(fields, funcs=PROCS, links=links, option=option)
    
    def create_item(self, option):
        ref = (self.name, option.get('nestedpath'))
        if not self._cache.get(ref):
            name = 'template'
            bases = (Item,)
            attrs = {}
            for field in option['fields']:
                attrs[field['key']] = Field(**field)
            t = type(name, bases, attrs)
            self._cache[ref] = t
        return self._cache[ref]


class Extractor:

    def __init__(self, tempalate, response):
        assert isinstance(tempalate, Template)
        self.template = tempalate
        self.response = response
        self.selector = response.selector
        self._cache = dict()

    @property
    def context(self):
        if not hasattr(self, '_context'):
            self._context = {
                '$url': self.response.url,
                '$time': time.time(),
                '$datetime': datetime.datetime.now(),
            }
        return self._context
    
    @property
    def items(self):
        if not hasattr(self, '_items'):
            self._items = []
        return self._items
    
    @property
    def links(self):
        if not hasattr(self, '_links'):
            self._links = []
        return self._links

    def execute(self):
        self._execute()
        return {
            'items': self.items,
            'links': self.links
        }

    def __extract(self, response, option):
        namespace = option.get('nestedpath', 'html')
        namespaces = response.css(namespace)
        context = self.context
        t = self.template.create_item(option)
        for selector in namespaces:
            template = t()
            l = ItemLoader(item=template, selector=selector, response=response)
            l.default_input_processor = Identity()
            l.default_output_processor = TakeFirst()
            for key, field in template.fields.items():
                path = field.get('path')
                pipelines = field.get('pipelines', [])
                try:
                    if path.startswith('$'):
                        value = context.get(path)
                        l.add_value(key, value, *pipelines, **field)
                    else:
                        l.add_css(key, path, *pipelines, **field)
                except Exception as e:
                    logger.error('Failed %s %s %s: %s', key, namespace, path, e)
            yield l.load_item()

    def __filter(self, output, conds, debug=False):
        filter_ = lambda item: item and filter_outout(item, conds, debug)
        return list(map(dict, filter(filter_, output)))

    def _extract(self, response, option):
        """
            1. extract links/items
            2. filter desired output
        """
        conds = option.get('conds', [])
        ref = option.get('ref')
        if not ref:
            extracts = self.__extract(response, option)
            _ITEM_REFS[response] = list(arg_to_iter(extracts))
        return self.__filter(_ITEM_REFS[response], conds)
    
    def _execute(self):
        """
            Parse with the first successful option
        """
        if not self.items:
            for option in self.template.items:
                output = self._extract(self.response, option)
                self.items.extend(output)
                if self.items:
                    break

        if not self.links:
            for option in self.template.links:
                suffix = option.get('suffix')
                callback = option['callback']
                priority = option.get('priority', 0)
                for link in self._extract(self.response, option):
                    link = dict(**link)
                    link.setdefault('callback', callback)
                    link.setdefault('priority', priority)
                    suffix = link.pop('suffix', '') or suffix
                    if suffix:
                        link['url'] = urljoin(link['url'], suffix)
                    self.links.append(link)


class TemplateFactory:

    def __init__(self, path):
        self.templates = {}
        self.load(path)

    def load(self, path=None, initialize=False):
        if initialize:
            self.templates.clear()
        parser = load_parser(path)
        self.start_urls = parser.get('start_urls', [])
        for callback in parser['callbacks']:
            self.register(callback)

    def register(self, callback):
        name = callback.get('name')
        self.templates[name] = Template(callback, name)

    def parse(self, response):
        template = self._get_template(response)
        return Extractor(template, response).execute()

    def _get_template(self, response):
        """
            select parser from pre-defined response's callback
        """
        callback = response.meta.get('callback')
        return self.templates[callback]