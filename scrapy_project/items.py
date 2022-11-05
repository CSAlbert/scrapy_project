# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScrapyProjectItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class QuoteItem(scrapy.Item):
    # define the fields for your item here like:
    text = scrapy.Field()
    author = scrapy.Field()
    tags = scrapy.Field()
    pass


# 网咖电竞大众点评点评
class WkOTADpCommentItem(scrapy.Item):
    # define the fields for your item here like:
    channelName = scrapy.Field()
    shopId = scrapy.Field()
    shopName = scrapy.Field()
    userNickName = scrapy.Field()
    userId = scrapy.Field()
    reviewId = scrapy.Field()
    addTime = scrapy.Field()
    updateTime = scrapy.Field()
    star = scrapy.Field()
    scoreMap = scrapy.Field()
    accurateStar = scrapy.Field()
    accurateScoreMap = scrapy.Field()
    content = scrapy.Field()
    avgPrice = scrapy.Field()
    shopReply = scrapy.Field()
    shopReplyTime = scrapy.Field()
    pass


# 网咖电竞美团点评
class WkOTAMtCommentItem(scrapy.Item):
    # define the fields for your item here like:
    channelName = scrapy.Field()
    shopId = scrapy.Field()
    shopName = scrapy.Field()
    userNickName = scrapy.Field()
    userId = scrapy.Field()
    reviewId = scrapy.Field()
    addTime = scrapy.Field()
    updateTime = scrapy.Field()
    star = scrapy.Field()
    accurateStar = scrapy.Field()
    content = scrapy.Field()
    shopReply = scrapy.Field()
    shopReplyTime = scrapy.Field()
    orderInfoDTOList = scrapy.Field()
    pass
