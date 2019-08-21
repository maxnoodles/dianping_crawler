# coding:utf-8
import random
import re
import time

import requests
import pymongo
from scrapy import Selector
import redis
from functools import partial
import json
from collections import defaultdict
from fake_useragent import UserAgent
import traceback
from concurrent.futures import ThreadPoolExecutor


class DianPingShopParse:

    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client['DianPing']
        self.r = redis.Redis(decode_responses=True)

        self.headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': '',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,'
                      'image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        self.cookie_set_name = 'dp_proxy_ua_cookies'

        # 要抓取的字段
        self.fields = ['url', 'shop_district', 'shop_type', 'shop_address', 'shop_phone', 'shop_hour', 'img',]

    def parse_movie(self, url, html):
        """
        url = 'https://maoyan.com/cinema/24089?movieId='
        """
        dic = dict()
        shop_address = html.xpath('//div[@class="address text-ellipsis"]/text()').get(default='')
        shop_phone = html.xpath('//div[@class="telphone"]/text()').get(default='')
        if shop_phone:
            shop_phone = shop_phone.replace('电话：', '')
        shop_district = ''
        shop_type = '电影院'
        shop_hour = '周一至周日 00:00-24:00'
        img = html.xpath('//div[@class="avatar-shadow"]/img/@src').re_first('.*[jpg|png]')
        if img and 'http' not in img:
            img = 'https:' + img

        for field in self.fields:
            try:
                dic[field] = eval(field)
            except (TypeError, NameError):
                pass

        dic['function_name'] = 'parse_movie'
        return dic

    def parse_normal(self, url, html):
        """url = 'https://m.dianping.com/shop/96637630'"""
        dic = dict()
        shop_district = html.xpath('//div[@class="shop-crumbs"]/a[2]/text()').get(default='')
        shop_type = html.xpath('//div[@class="shop-crumbs"]/a[3]/text()').get(default='')
        json_data = html.xpath('//textarea[@id="shop-detail"]/text()').get(default='')
        if json_data:
            json_data = json.loads(json_data)
            shop_address = json_data.get('address')
            shop_phone = json_data.get('phoneNum')
        else:
            shop_address = ''
            shop_phone = ''
        shop_hour = html.xpath('//div[@class="businessHour"]/text()').get(default='')
        if shop_hour:
            shop_hour = shop_hour.strip().replace('\n', ' ')
        img = html.xpath('//div[@class="picAndNum"]/img/@src').re_first(r'(.*?(jpg|png))', default='')
        if img and 'http' not in img:
            img = 'https:' + img

        for field in self.fields:
            try:
                dic[field] = eval(field)
            except (TypeError, NameError):
                dic[field] = ''

        dic['function_name'] = 'parse_normal'
        return dic

    def parse_not_phone(self, url, html):
        """
        url = 'https://m.dianping.com/shop/96572783'
        特点: shop_phone = ''
        """
        dic = dict()
        shop_district = html.xpath('//div[@class="info"]//span[1]/text()').get(default='')
        shop_type = html.xpath('//div[@class="info"]//span[2]/text()').get(default='')
        shop_address = html.xpath('//i[@class="i-add"]/following-sibling::span/text()').get(default='')
        shop_phone = html.xpath('//a[@id="telphone"]/@href').re_first(r'\d+', default='')

        shop_hour = html.xpath('//section[@class="shopinfo-details"]//article[@class="content"]/p/text()').get(default='')
        if shop_hour:
            shop_hour = shop_hour.strip().replace('\n', ' ')

        img = html.xpath('//img/@src').re_first(r'(.*?(jpg|png))', default='')
        if img and 'http' not in img:
            img = 'https:' + img

        for field in self.fields:
            try:
                dic[field] = eval(field)
            except (TypeError, NameError):
                dic[field] = ''

        dic['function_name'] = 'parse_not_phone'
        return dic

    def parse_not_hour(self, url, resp_text, html):
        """
        url2 = 'https://m.dianping.com/shop/93784395'
        特点: shop_hour = ''
        """
        dic = dict()
        shop_district = html.xpath('//span[@class="region"]/text()').get(default='')
        if '老师' in resp_text or '课程' in resp_text:
            shop_type = '教育'
        else:
            shop_type = ''
        shop_info = html.xpath('//div[@class="link-box"]/p[1]//text()')
        if len(shop_info):
            shop_address = shop_info[0].get()
            shop_phone = shop_info[1].get()
        else:
            shop_address = ''
            shop_phone = ''
        shop_hour = ''

        img = html.xpath('//img[@class="J-piczoom"]/@src').re_first(r'(.*?(jpg|png))', default='')
        if img and 'http' not in img:
            img = 'https:' + img

        for field in self.fields:
            try:
                dic[field] = eval(field)
            except (TypeError, NameError):
                dic[field] = ''

        dic['function_name'] = 'parse_not_hour'
        return dic

    def parse_not_district(self, url, resp_text, html):
        """
        url3 = 'https://m.dianping.com/shop/97140815'
        特点: shop_district = ''
        """
        dic = dict()
        shop_district = ''
        if '老师' in resp_text or '课程' in resp_text:
            shop_type = '教育'
        else:
            shop_type = ''
        shop_address = html.xpath('//p[@class="main-address"]/text()').get(default='')
        shop_phone = html.xpath('//a[@class="phone"]/@href').get(default='')
        if shop_phone:
            shop_phone = shop_phone.strip()
            shop_phone = shop_phone.split(':')[1]

        shop_hour = html.xpath('//section[@class="mod shop-detail"]/div[@class="item"]/div/text()').get(default='')
        if shop_hour:
            shop_hour = shop_hour.strip().replace('\n', ' ')

        img = html.xpath('//div[@class="wrapper"]//img/@src|//*[@class="pic"]//img/@lazy-src').re_first(r'(.*?(jpg|png))', default='')
        if img and 'http' not in img:
            img = 'https:' + img

        for field in self.fields:
            try:
                dic[field] = eval(field)
            except (TypeError, NameError):
                dic[field] = ''

        dic['function_name'] = 'parse_not_district'
        return dic

    def parse_not_phone_two(self, url, html):
        """
        url4 = 'https://m.dianping.com/shop/93505858'
        特点: shop_phone = ''
        """
        dic = dict()
        shop_district = html.xpath('//span[@class="region"]/text()').get(default='')
        shop_address = html.xpath('////p[@class="address"]/text()').get(default='')
        shop_phone = ''
        shop_hour = html.xpath('//p[@class="list-item"]//text()').get(default='')
        if shop_hour:
            shop_hour = shop_hour.strip().replace('\n', ' ')

        img = html.xpath('//div[@class="shopDetail"]//img/@src').re_first(r'(.*?(jpg|png))', default='')
        if img and 'http' not in img:
            img = 'https:' + img

        for field in self.fields:
            try:
                dic[field] = eval(field)
            except (TypeError, NameError):
                dic[field] = ''

        dic['function_name'] = 'parse_not_phone_two'
        return dic

    def parse_not_phone_hour_district(self, url, resp_text, html):
        """
        url5 = 'https://m.dianping.com/shop/96572868'
        特点: shop_phone = ''
        特点: shop_hour = ''
        特点: shop_district = ''
        """
        dic = dict()
        shop_district = ''
        if '老师' in resp_text or '课程' in resp_text:
            shop_type = '教育'
        else:
            shop_type = ''
        # shop_address = html.xpath('//article[@class="add bottom-border"]//text()').get(default='')
        shop_address = re.search('<i class="i-add"></i>(.*?)<i class="arrow-ent right"></i>', resp_text, re.S)
        if shop_address:
            shop_address = shop_address.group(1)

        shop_phone = html.xpath('//a[@id="telphone"]/@href').re_first(r'\d+', default='')
        shop_hour = html.xpath('//*[@class="sersice"]/p/text()').get(default='')
        if shop_hour:
            shop_hour = re.sub('\n', '', shop_hour)

        img = html.xpath('//*[@class="pic"]//img/@src').re_first(r'(.*?(jpg|png))', default='')
        if img and 'http' not in img:
            img = 'https:' + img

        for field in self.fields:
            try:
                dic[field] = eval(field)
            except (TypeError, NameError):
                dic[field] = ''

        dic['function_name'] = 'parse_not_phone_hour_district'
        return dic

    def parse_resp(self, url, resp_text):
        html = Selector(text=resp_text)
        try:
            if 'movieId' in url or 'maoyan' in url:
                dic = self.parse_movie(url, html)
            else:
                dic = self.parse_normal(url, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_phone(url, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_hour(url, resp_text, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_district(url, resp_text, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_phone_two(url, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_phone_hour_district(url, resp_text, html)
            return dic
        except Exception as e:
            print('解析出错', e)

    @staticmethod
    def update_mongo(col, url, dic):
        col.update_one({'url': url},
                            {'$set': {'address': dic['shop_address'],
                                      'area': dic['shop_district'],
                                      'phoneNum': dic['shop_phone'],
                                      'dobusiness': dic['shop_hour'],
                                      'category': dic['shop_type'],
                                      'thumbUrl': dic['img']
                                      }},)

    def get_and_parse_and_mongo(self, key, url, city_en):
        resp_text = self.r.hget(key, url)
        dic = self.parse_resp(url, resp_text)
        if dic:
            col = self.db[f'dp_{city_en}_shop']
            self.update_mongo(col, url, dic)
            print(dic)
            return dic

    def get_redis_url(self, key):
        shop_urls = self.r.hkeys(key)
        return shop_urls

    def run(self):
        city_en = 'guangzhou'
        key = f'dp_{city_en}_text'
        shop_urls = self.get_redis_url(key=key)
        for shop_url in shop_urls:
            self.get_and_parse_and_mongo(key, shop_url, city_en)

        self.client.close()


def main():
    spider = DianPingShopParse()
    spider.run()


if __name__ == "__main__":
    main()









