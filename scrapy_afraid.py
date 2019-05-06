import scrapy
from scrapy.loader.processors import Compose, TakeFirst
from urllib.parse import urlparse
import datetime
import dataset
import os
import sys

# Use `scrapy runspider __name__.py` to scrape.

# Exteremly verbose UserAgent string
UA = f'Scrapy/{scrapy.__version__} Python/{".".join(map(lambda x: str(x), sys.version_info[0:3]))} (github.com/zvodd/scrapy-example-afraid.org)'
DEFAULT_START_URL = 'http://freedns.afraid.org/domain/registry/'
DB_CONNECT = 'sqlite:///output.sqlite'

# # Used mitmproxy to inspect http traffic during testing.
# # TODO find alternitive proxy / mitm plugin for caching or "smart replay".
# PROXIES_ENV = {
#     # "http_proxy": "http://127.0.0.1:8080",
#     # "https_proxy": "http://127.0.0.1:8080"
# }
# os.environ.update(PROXIES_ENV)


class AfraidSpider(scrapy.Spider):
    name = 'afraidspider'
    start_urls = [DEFAULT_START_URL]
    custom_settings = {
        "USER_AGENT": UA,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750
        },
        'ITEM_PIPELINES': {
            # Seems to be a solution for single file spider middlerware.
            __name__ + '.DomainDataSetPipeline': 400,
        }
    }

    def __init__(self, start_url=None, **kwargs):
        if start_url:
            self.start_urls = [start_url]
        super().__init__(**kwargs)

    def parse(self, response):
        table = response.xpath('/html/body/table/tr[2]/td[2]/center/center/table')
        rows = table.css('tr:nth-child(n+4)')
        lastrow = rows.pop()
        pagenum = lastrow.xpath('.//input[@name="page"]/@value').get()
        for row in rows:
            l = DomainItemLoader(item=DomainItem(), response=response, selector=row)
            l.add_xpath('Domain', './td[1]/a/text()')
            l.add_xpath('NumHosts', './td[1]/span/text()')
            l.add_xpath('Status', './td[2]/text()')
            l.add_xpath('Owner', './td[3]/a/text()')
            l.add_xpath('Age', './td[4]/text()')
            l.add_value('FromPage', pagenum)
            yield l.load_item()

        next_page = lastrow.xpath("./td/table/tr/td[2]/a/@href").get()
        if next_page:
            yield {"page": urlparse.urljoin(response.url, next_page)}
            yield response.follow(next_page, self.parse)




class DomainItemLoader(scrapy.loader.ItemLoader):
    default_output_processor = TakeFirst()

    # Compose joins the passed in list, `join` because we iterate over each character.
    NumHosts_in = Compose(lambda x: ''.join((y for y in x if y.isdigit())))

    @staticmethod
    def Age_out_func(alist):
        astr = ''.join(alist)
        dt_fstring = r"%m/%d/%Y"
        ax = astr.split("(").pop().replace(')', '')
        return datetime.datetime.strptime(ax, dt_fstring).date()

    Age_out = Age_out_func



class DomainItem(scrapy.Item):
    Domain = scrapy.Field()
    NumHosts = scrapy.Field()
    Site = scrapy.Field()
    Status = scrapy.Field()
    Owner = scrapy.Field()
    Age = scrapy.Field()
    FromPage = scrapy.Field()


class DomainDataSetPipeline(object):
    def __init__(self):
        db = dataset.connect(DB_CONNECT)
        table = db['domains']
        self.table = table
        self.db = db

    def process_item(self, item, spider):
        self.table.insert(item)
        return item


# class NextPagePipeline(object):
#     def __init__(self):
#         db = dataset.connect(DB_CONNECT)
#         table = db['next_page']
#         self.table = table
#         self.db = db

#     def process_item(self, item, spider):
#         self.table.insert(item)

#     def open_spider(self, spider):
#         spider.NextPagePipeline = self
