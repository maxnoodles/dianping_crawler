# coding:utf-8
import re
import time

import requests
import pymongo
from scrapy import Selector
import redis
from functools import partial
import json
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

requests.packages.urllib3.disable_warnings()

class DianPingShopSpider:

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
        self.ua_cookie_set = 'abuyun_cookies'

        # 要抓取的字段
        self.fields = ['url', 'shop_district', 'shop_type', 'shop_address', 'shop_phone', 'shop_hour', 'img']

        proxyHost = "http-dyn.abuyun.com"
        proxyPort = "9020"

        # 代理隧道验证信息
        proxyUser = "HAOD93TI7TC5T35D"
        proxyPass = "86C755D8D83FD92B"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxyHost,
            "port": proxyPort,
            "user": proxyUser,
            "pass": proxyPass,
        }

        self.proxies = {
            "http": proxyMeta,
            "https": proxyMeta,
        }

    def get_urls(self, col):
        """
        获取 mongodb 中 url
        :return:
        """
        shops = col.find({'address': None})
        shop_urls = [shop['url'] for shop in shops]
        return shop_urls

    def parse_movie(self, response, html):
        """
        url = 'https://maoyan.com/cinema/24089?movieId='
        """
        dic = dict()
        url = response.url
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

    def parse_normal(self, response, html):
        """url = 'https://m.dianping.com/shop/96637630'"""
        dic = dict()
        url = response.url
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

    def parse_not_phone(self, response, html):
        """
        url = 'https://m.dianping.com/shop/96572783'
        特点: shop_phone = ''
        """
        dic = dict()
        url = response.url
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

    def parse_not_hour(self, response, html):
        """
        url2 = 'https://m.dianping.com/shop/93784395'
        特点: shop_hour = ''
        """
        dic = dict()
        url = response.url
        shop_district = html.xpath('//span[@class="region"]/text()').get(default='')
        if '老师' in response.text or '课程' in response.text:
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

    def parse_not_district(self, response, html):
        """
        url3 = 'https://m.dianping.com/shop/97140815'
        特点: shop_district = ''
        """
        dic = dict()
        url = response.url
        shop_district = ''
        if '老师' in response.text or '课程' in response.text:
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

    def parse_not_phone_two(self, response, html):
        """
        url4 = 'https://m.dianping.com/shop/93505858'
        特点: shop_phone = ''
        """
        dic = dict()
        url = response.url
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

    def parse_not_phone_hour_district(self, response, html):
        """
        url5 = 'https://m.dianping.com/shop/96572868'
        特点: shop_phone = ''
        特点: shop_hour = ''
        特点: shop_district = ''
        """
        dic = dict()
        url = response.url
        shop_district = ''
        if '老师' in response.text or '课程' in response.text:
            shop_type = '教育'
        else:
            shop_type = ''
        # shop_address = html.xpath('//article[@class="add bottom-border"]//text()').get(default='')
        shop_address = re.search('<i class="i-add"></i>(.*?)<i class="arrow-ent right"></i>', response.text, re.S)
        if shop_address:
            shop_address = shop_address.group(1)
        else:
            shop_address = ''

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

    def parse_apartment(self, response, html):
        """
        url5 = 'https://m.dianping.com/shop/126291787'
        特点: shop_phone = ''
        特点: shop_hour = ''
        """
        dic = dict()
        url = response.url

        if 'incp-hotel' in response.text:
            new_url = url.replace(r'shop/', 'hotelm/ajax/hotelInfo?shopId=')

            _, ua, cookies, all_json = self.get_proxy_ua_cookies(self.ua_cookie_set)
            self.headers['User-Agent'] = ua
            resp = requests.get(new_url, headers=self.headers, proxies=self.proxies)
            resp_json = resp.json()

            if resp_json.get('msg') == 'OK':
                data = resp_json.get('data')
                if data:
                    shop_district = data.get('city')
                    shop_type = data.get('categoryName') + '酒店'
                    shop_address = data.get('address')
                    img = data.get('defaultPic')
                    if img and 'http' not in img:
                        img = 'https:' + img
                    origin_img = re.search(r'.*?(jpg|png)', img)
                    if origin_img:
                        img = origin_img.group(0)

            for field in self.fields:
                try:
                    dic[field] = eval(field)
                except (TypeError, NameError):
                    dic[field] = ''

            dic['function_name'] = 'parse_apartment'
            return dic

    def get_proxy_ua_cookies(self, key):
        """
        根据传入的redis key名去对应的key中取proxy, ua, cookies
        :param key: redis.key
        :return: proxy, ua, cookies, all_json
        """
        while True:
            if self.r.scard(key):
                break
            time.sleep(10)

        all_json = self.r.srandmember(key)
        all_dict = json.loads(all_json)

        if all_dict.get('proxy'):
            proxy = all_dict.get('proxy')
            proxy = {
                'http': 'http://' + proxy,
                'https': 'https://' + proxy,
            }
        else:
            proxy = None

        ua = all_dict.get('ua')
        cookies = all_dict.get('cookies')
        return proxy, ua, cookies, all_json

    def get_resp(self, url, headers, cookies, proxy, all_json=None):
        """

        :param url:
        :param headers:
        :param cookies:
        :param proxy:
        :param all_json: json 方便删除从 redis set中删除数据
        :return:
        """
        try:
            resp = requests.get(url=url, headers=headers, cookies=cookies, proxies=proxy, timeout=10, verify=False)
        except Exception as e:
            print(e)
        else:
            if resp.status_code == 200 and 'verify' not in resp.url:
                return resp
            else:
                print(f'状态码错误 {resp.status_code}, 请输入验证码', url)
                # 删除进验证，而非出错的cookies
                if all_json:
                    self.r.srem(self.ua_cookie_set, all_json)
                return False

    def parse_resp(self, resp):
        html = Selector(text=resp.text)
        try:
            if 'movieId' in resp.url or 'maoyan' in resp.url:
                dic = self.parse_movie(resp, html)
            else:
                dic = self.parse_normal(resp, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_phone(resp, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_hour(resp, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_district(resp, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_phone_two(resp, html)
                if dic['shop_address'] == '':
                    dic = self.parse_not_phone_hour_district(resp, html)
                if dic['shop_address'] == '':
                    dic = self.parse_apartment(resp, html)
            return dic
        except Exception as e:
            print('解析出错', e)

    def update_mongo(self, url, dic, col):
        col.update_one({'url': url},
                        {'$set': {'address': dic['shop_address'],
                                  'area': dic['shop_district'],
                                  'phoneNum': dic['shop_phone'],
                                  'dobusiness': dic['shop_hour'],
                                  'category': dic['shop_type'],
                                  'thumbUrl': dic['img']
                                  }},)

    def get_and_parse_and_mongo(self, url, col):
        proxy, ua, cookies, all_json = self.get_proxy_ua_cookies(self.ua_cookie_set)
        self.headers['User-Agent'] = ua

        paoxy = self.proxies

        resp = self.get_resp(url, self.headers, cookies, paoxy, all_json)
        if resp:
            dic = self.parse_resp(resp)
            if dic:
                self.update_mongo(url, dic, col)
                print(dic)
                return dic

    def get_redis_url(self, key):
        shop_urls = self.r.smembers(key)
        return shop_urls

    def redis_run(self):
        shop_urls = self.get_redis_url(key='dp_guangzhou_shops')
        for shop_url in shop_urls:
            if self.r.sismember('dp_finish', shop_url):
                continue
            dic = self.get_and_parse_and_mongo(shop_url)
            if dic:
                print(dic['function_name'], dic)

        self.client.close()

    def supplement_shop_info(self, data, store_id):
        url = f'https://m.dianping.com/shop/{store_id}'
        proxy, ua, cookies, all_json = self.get_proxy_ua_cookies(self.ua_cookie_set)
        self.headers['User-Agent'] = ua
        paoxy = self.proxies
        resp = self.get_resp(url, self.headers, cookies, paoxy, all_json)
        if resp:
            dic = self.parse_resp(resp)
            try:
                for k, v in dic.items():
                    if k and k in self.fields:
                        data[k] = v
                print(data)
                return data
            except:
                pass

    def excel_run(self):
        path = r'C:Users\Administrator\Desktop\20190802_shopid_title(2)(1).xls'
        df = pd.read_excel(path)
        col_name = df.columns.tolist()
        col_name.extend(self.fields)
        df.reindex(columns=col_name, )
        df.drop_duplicates('store_id', 'first', True)
        df = df.apply(lambda x: self.supplement_shop_info(x, x['store_id']), axis=1)
        df.to_excel(path, index=False)
        print(df)

    def run(self):
        for city_en in ['dongguan', 'guangzhou', 'foshan']:
            col = self.db[f'dp_{city_en}_shop']
            shop_urls = self.get_urls(col)
            with ThreadPoolExecutor(max_workers=2) as executor:
                executor.map(partial(self.get_and_parse_and_mongo, col=col), shop_urls)

        self.client.close()


def main():
    spider = DianPingShopSpider()
    # spider.run()
    spider.excel_run()


if __name__ == "__main__":
    main()









