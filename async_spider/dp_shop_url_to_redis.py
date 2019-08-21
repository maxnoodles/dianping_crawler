import redis
import pymongo


class DianPingShopUrlToRedis:

    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client['DianPing']
        self.city_col = self.db['dp_city']

        self.r = redis.StrictRedis(decode_responses=True)

        self.city_lists = ['广州', '东莞', '佛山']

    def get_city_info(self):
        city_infos = self.city_col.find({'name': {'$in': self.city_lists}})
        city_info_list = []
        for city in city_infos:
            del city['_id']
            city_info_list.append(city)
        return city_info_list

    def get_city_shop_url(self, city_name):
        col = self.db[f'dp_{city_name}_shop']
        city_result = col.find({})
        city_shop_urls = [i['url'] for i in city_result]
        return city_shop_urls

    def url_to_redis(self, city_name, url):
        set_name = f'dp_{city_name}_shops'
        self.r.sadd(set_name, url)

    def main(self):
        city_info_list = self.get_city_info()
        for city_info in city_info_list:
            print(city_info)
            city_en_name = city_info['en_name']
            city_shop_urls = self.get_city_shop_url(city_en_name)
            for url in city_shop_urls:
                self.url_to_redis(city_en_name, url)
        self.client.close()


def run():
    to_redis = DianPingShopUrlToRedis()
    to_redis.main()


if __name__ == '__main__':
    run()

