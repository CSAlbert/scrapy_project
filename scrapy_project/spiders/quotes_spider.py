# -*- coding: utf-8 -*-
"""
@Author   : chulang
@DateTime : 2022/10/11 11:16
@File     : quotes_spider.py
@Describe : TODO
"""

import scrapy

from scrapy_project.items import QuoteItem


class QuotesSpider(scrapy.Spider):
    name = "quotes"

    # start_urls = [
    #     'http://quotes.toscrape.com/page/1/',
    # ]

    def start_requests(self):
        urls = [
            'http://quotes.toscrape.com/page/1/',
            # 'http://quotes.toscrape.com/page/2/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.quote_parse)

    def quote_parse(self, response, **kwargs):
        # 保存整个页面html到文件
        # page = response.url.split("/")[-2]
        # filename = f'quotes-{page}.html'
        # with open(filename, 'wb') as f:
        #     f.write(response.body)
        # self.log(f'Saved file {filename}')

        # 输出三类信息
        # for quote in response.css('div.quote'):
        #     yield {
        #         'text': quote.css('span.text::text').get(),
        #         'author': quote.css('small.author::text').get(),
        #         'tags': quote.css('div.tags a.tag::text').getall(),
        #     }
        list_quotes = response.css('div.quote')

        for one_quote in list_quotes:
            text = one_quote.css('span.text::text').get()
            author = one_quote.css('small.author::text').get()
            tags = one_quote.css('div.tags a.tag::text').get()

            # 将爬取到的名言保存到item中
            # 定义QuoteItem对象
            item = QuoteItem()
            item["text"] = text
            item["author"] = author
            item["tags"] = tags

            print(item)
            # 使用yield返回item
            yield item

        # 递归跟踪下一页链接
        next_page = response.css('li.next a::attr(href)').get()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.quote_parse)
