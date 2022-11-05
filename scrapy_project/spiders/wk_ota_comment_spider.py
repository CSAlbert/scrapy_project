# -*- coding: utf-8 -*-
"""
@Author   : chulang
@DateTime : 2022/11/2 14:58
@File     : wk_ota_comment_spider.py
@Describe : 采集电竞OTA管理后台点评明细数据
            包括大众点评、美团，由于部分字段不同分两个Item获取数据，存储到MongoDB的同一个表
            采集日期：根据配置文件中的spider_days（近n天的）
"""
import json
import math
from datetime import datetime, timedelta

import scrapy

from scrapy_project import settings
from scrapy_project.items import WkOTADpCommentItem, WkOTAMtCommentItem


class WkOTACommentSpider(scrapy.Spider):
    name = 'wk_ota_comment_spider'

    # 指定pipeline
    custom_settings = {
        'ITEM_PIPELINES': {'scrapy_project.pipelines.WkOTACommentToMongoDB': 300}
    }

    def start_requests(self):
        spider_days = settings.SPIDER_DAY
        print(spider_days)
        # today = datetime.today().strftime("%Y-%m-%d")
        begin_day = ((datetime.now()) + timedelta(days=-spider_days)).strftime("%Y-%m-%d")
        end_day = ((datetime.now()) + timedelta(days=-1)).strftime("%Y-%m-%d")

        urls = [
            # 点评
            'https://e.dianping.com/review/app/index/ajax/pcreview/list?platform=0&shopId=0&tagId=0&startDate=' + begin_day + '&endDate=' + end_day + '&pageNo=1&pageSize=10&referType=0&category=1',
            # 美团
            'https://e.dianping.com/review/app/index/ajax/pcreview/list?platform=1&shopId=0&tagId=0&startDate=' + begin_day + '&endDate=' + end_day + '&pageNo=1&pageSize=10&referType=0&category=1',
        ]

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
        }

        with open(r'D:\PycharmProjects\pythonProject\wy\infile\wk_mt_cookies.txt', 'r', encoding='utf8') as f:
            cookie_dict = json.loads(f.read())
            # print(cookie_dict)
        cookies = {}
        # 往driver里添加cookies
        for cookie in cookie_dict:
            cookies[cookie['name']] = cookie['value']

        for url in urls:
            if 'platform=0' in url:
                platform = 0
            else:
                platform = 1
            print(url)
            yield scrapy.Request(url=url, headers=headers, cookies=cookies, callback=self.url_parse, dont_filter=True,
                                 meta={'begin_day': begin_day, 'end_day': end_day, 'platform': platform})

    def url_parse(self, response, **kwargs):
        """
        通过总评论条数，解析需要爬取的页面数，拼接成url，提交到引擎
        :param response:
        :return:
        """
        begin_day = response.meta['begin_day']
        end_day = response.meta['end_day']
        platform = response.meta['platform']

        page_num = 0
        json_data = json.loads(response.text)
        total_nums = json_data['msg']['totalReivewNum']
        page_nums = math.ceil(total_nums // 10)
        while page_num < page_nums:
            page_num += 1
            page_url = 'https://e.dianping.com/review/app/index/ajax/pcreview/list?platform=' + str(
                platform) + '&shopId=0&tagId=0&startDate=' + begin_day + '&endDate=' + end_day + '&pageNo=' + str(
                page_num) + '&pageSize=10&referType=0&category=1'
            print(page_url)
            yield scrapy.Request(page_url, callback=self.comment_parse, dont_filter=True, meta={'platform': platform})

    def comment_parse(self, response):
        """
        解析每个url请求返回的json数据，保存到Item
        :param response:
        :return:
        """
        platform = response.meta['platform']
        print('------------------------单页开始------------------------')
        print(response.url)
        print(response.text)
        json_data = json.loads(response.text)
        print(json_data)
        comments = json_data['msg']['reviewDetailDTOs']

        # platform == 0 点评
        if platform == 0:
            for one_comment in comments:
                print(one_comment)
                shopId = one_comment['shopId']
                shopName = one_comment['shopName']
                userNickName = one_comment['userNickName']
                userId = one_comment['userId']
                reviewId = one_comment['reviewId']
                addTime = one_comment['addTime']
                updateTime = one_comment['updateTime']
                star = one_comment['star']
                scoreMap = one_comment['scoreMap']
                accurateStar = one_comment['accurateStar']
                accurateScoreMap = one_comment['accurateScoreMap']
                content = one_comment['content']
                avgPrice = one_comment['avgPrice']
                if one_comment['reviewFollowNoteDtoList'] is None:
                    shopReply = None
                    shopReplyTime = None
                else:
                    shopReply = one_comment['reviewFollowNoteDtoList'][0]['noteBody']
                    shopReplyTime = one_comment['reviewFollowNoteDtoList'][0]['addDate']

                item = WkOTADpCommentItem()
                item['channelName'] = '大众点评'
                item['shopId'] = shopId
                item['shopName'] = shopName
                item['userNickName'] = userNickName
                item['userId'] = userId
                item['reviewId'] = reviewId
                item['addTime'] = addTime
                item['updateTime'] = updateTime
                item['star'] = star
                item['scoreMap'] = scoreMap
                item['accurateStar'] = accurateStar
                item['accurateScoreMap'] = accurateScoreMap
                item['content'] = content
                item['avgPrice'] = avgPrice
                item['shopReply'] = shopReply
                item['shopReplyTime'] = shopReplyTime

                print(item)
                yield item
        else:
            # platform == 1 美团
            for one_comment in comments:
                print(one_comment)
                shopId = one_comment['shopId']
                shopName = one_comment['shopName']
                userNickName = one_comment['userNickName']
                userId = one_comment['userId']
                reviewId = one_comment['reviewId']
                addTime = one_comment['addTime']
                updateTime = one_comment['updateTime']
                star = one_comment['star']
                accurateStar = one_comment['accurateStar']
                content = one_comment['content']
                shopReply = one_comment['shopReply']
                shopReplyTime = one_comment['shopReplyTime']
                orderInfoDTOList = one_comment['orderInfoDTOList']

                item = WkOTAMtCommentItem()
                item['channelName'] = '美团'
                item['shopId'] = shopId
                item['shopName'] = shopName
                item['userNickName'] = userNickName
                item['userId'] = userId
                item['reviewId'] = reviewId
                item['addTime'] = addTime
                item['updateTime'] = updateTime
                item['star'] = star
                item['accurateStar'] = accurateStar
                item['content'] = content
                item['shopReply'] = shopReply
                item['shopReplyTime'] = shopReplyTime
                item['orderInfoDTOList'] = orderInfoDTOList
                print(item)
                yield item
        print('------------------------单页结束------------------------')
