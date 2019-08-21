import requests
import pymongo
import redis
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

class DianPingShopSpider:

    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client['DianPing']
        self.r = redis.StrictRedis(decode_responses=True)

        self.finish_set = 'finish_mall_simple'
        self.not_finish_set = 'not_finish_mall_simple'

        self.headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }

        self.base_url = 'https://mapi.dianping.com/shopping/mall/shops?mallid={mallid}&page={pg}&pagesize=50'
        self.totalCount = 0
        # self.city_lists = ['guangzhou', 'dongguan', 'foshan', 'huizhou', 'zhuhai', 'zhongshan']
        self.city_lists = ['guangzhou', 'dongguan', 'foshan']
        self.city_shop_count = defaultdict(int)

    def get_data(self, city_mall_tuple, pg):
        url = self.base_url.format(mallid=city_mall_tuple[0], pg=pg)
        self.r.sadd(self.not_finish_set, url)
        if self.r.sismember(self.finish_set, url):
            print(f'该{url}已经存在，跳过请求')
            return False
        res = requests.get(url=url, headers=self.headers)
        if res.status_code == 200:
            res_json = res.json()
            msg = res_json.get('msg')
            if msg and msg != 'null':
                self.city_shop_count[city_mall_tuple[2]] += msg['totalCount']
                print(self.totalCount)
                shops = msg.get('shops')
                for shop in shops:
                    dic = dict()
                    dic['mall_name'] = city_mall_tuple[1]
                    dic['mall_id'] = city_mall_tuple[0]
                    dic['shop_id'] = shop.get('shopId')
                    dic['shop_name'] = shop.get('shopName')
                    dic['avgPrice'] = shop.get('avgPrice')
                    dic['floor'] = shop.get('floor')
                    dic['url'] = 'https:' + shop.get('url')
                    dic['cover'] = shop.get('cover')
                    print(dic)
                    shop_col = self.db[f'dp_{city_mall_tuple[2]}_shop']
                    self.save_to_mongo(dic, shop_col)
                    self.r.sadd(self.finish_set, url)
                pageCount = msg.get('pageCount')
                if pageCount and pg < pageCount:
                    pg += 1
                    self.get_data(city_mall_tuple, pg)

    def count_shops(self, mailid):
        url = self.base_url.format(mallid=mailid, pg=1)
        res = requests.get(url=url, headers=self.headers)
        if res.status_code == 200:
            res_json = res.json()
            msg = res_json.get('msg')
            if msg and msg != 'null':
                totalCount = msg['totalCount']
                return totalCount


    @staticmethod
    def save_to_mongo(dic, col):
        if '_shop_id' not in col.index_information():
            col.create_index('shop_id', name='_shop_id')
        col.update_one({'shop_id': dic['shop_id']}, {'$set': dic}, upsert=True)
        return

    def get_city_mall_tuples(self):
        all_mall_tuples = []
        for city in self.city_lists:
            city_col = self.db[f'dp_{city}_mall']
            malls_mongo = city_col.find({})
            malls = [(mail['mall_id'], mail['fullName'], city) for mail in malls_mongo]
            all_mall_tuples.extend(malls)
        print(all_mall_tuples)
        return all_mall_tuples

    def main(self):
        city_mall_tuples = spider.get_city_mall_tuples()
        # for city_mall_tuple in city_mall_tuples:
            # self.get_data(city_mall_tuple, 1)
        # print(self.city_shop_count)
        for city_name in self.city_lists[1:]:
            city_shop_count = 0
            for city_mall_tuple in city_mall_tuples:
                count_shops = self.count_shops(city_mall_tuple[0])
                if count_shops:
                    # print(city_mall_tuple[1], count_shops)
                    city_shop_count += count_shops
            print(city_name, city_shop_count)



if __name__ == '__main__':
    spider = DianPingShopSpider()
    spider.main()

