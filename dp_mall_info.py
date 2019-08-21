import re

import pandas as pd
import requests
import pymongo
from concurrent.futures import ThreadPoolExecutor


class DianPingMallSpider:

    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client['DianPing']
        self.city_col = self.db['dp_city']

        self.headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }

        self.base_url = 'https://mapi.dianping.com/shopping/mall/channel/nearbylist?cityid={}&page={}&pagesize=100'
        self.city_lists = ['广州', '东莞', '佛山', '惠州', '珠海', '中山']

    def get_city_mall_info(self, city_id, city_name, city_en_name, pg):
        col = self.client['DianPing'][f'dp_{city_en_name}_mall']
        url = self.base_url.format(city_id, pg)
        response = requests.get(url, headers=self.headers)
        response = response.json()
        msg = response.get('msg')
        if msg != 'null':
            mallList = msg.get('mallList')
            for mall in mallList:
                dic = dict()
                mall_banner = mall.get('defaultPic')
                mall_banner = re.search('.*?(jpg|jpeg|png)', mall_banner).group(0)
                dic['mall_banner'] = mall_banner

                dic['mall_id'] = mall.get('id')
                dic['mall_city'] = city_name
                dic['Tags'] = mall.get('mallTags')
                if dic['Tags']:
                    dic['Tags'] = dic['Tags'][-1]
                else:
                    dic['Tags'] = ''
                dic['score'] = mall.get('score')
                dic['hits'] = mall.get('hits')
                dic['fullName'] = mall.get('fullName')
                dic['mallActivity'] = mall.get('mallActivity')
                if dic['mallActivity']:
                    dic['mallActivity'] = dic['mallActivity'][-1].get('content')
                else:
                    dic['mallActivity'] = ''
                print(dic)
                if 'mall_id_1' not in col.index_information().keys():
                    col.create_index('mall_id', unique=True)
                col.update_one({'mall_id': dic['mall_id']}, {'$set': dic}, upsert=True)
            mall_total = msg.get('total')
            page_count = mall_total // 100  # 5
            if pg <= page_count:
                pg += 1
                print(url)
                self.get_city_mall_info(city_id, city_name, city_en_name, pg)

    def choice_city(self):
        city_infos = self.city_col.find({'name': {'$in': self.city_lists}})
        city_info_list = []
        for city in city_infos:
            del city['_id']
            city_info_list.append(city)
        return city_info_list

    def main(self):
        city_info_list = spider.choice_city()
        with ThreadPoolExecutor(max_workers=10) as executor:
            for city in city_info_list:
                print(city)
                executor.submit(self.get_city_mall_info, city['city_id'], city['name'], city['en_name'], 1)
        self.client.close()


if __name__ == '__main__':
    spider = DianPingMallSpider()
    spider.main()

