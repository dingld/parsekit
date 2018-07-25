from scrapy.loader import ItemLoader as _ItemLoader
from scrapy.utils.misc import arg_to_iter, flatten


class ItemLoader(_ItemLoader):

    def _get_cssvalues(self, csss, **kw):
        self._check_selector_method()
        csss = arg_to_iter(csss)
        return flatten(self.select(css, **kw).extract() for css in csss)

    def select(self, css, **kw):
        """
        Enhance datatype extraction
        """
        sel = self.selector.css(css)
        if any(isinstance(x.root, str) for x in sel):
            return sel
        t = kw.get('datatype', 'innertext')
        if t == 'url':
            t = 'href'
        if t in ['href', 'src', 'title', 'link']:
            return sel.xpath('.//@%s' % t)
        elif t == 'innertext':
            return sel.xpath('string()')
        else:
            return sel
