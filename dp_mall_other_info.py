import json
import re
from urllib.parse import unquote
import logging
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from scrapy import Selector
import pymongo
import redis
from fake_useragent import UserAgent

from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logging.basicConfig(filename='dp_mall_other_error.log')
logger = logging.getLogger()

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': UserAgent().random,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}


# 停车信息
base_park_url = 'https://m.dianping.com/shopping/node/shop/parkinglot/{}'
# 服务信息
base_service_url = 'https://m.dianping.com/shopping/node/shop/shopbaseinfo/{}'
# 商场活动
base_activity_url = 'https://m.dianping.com/shoppingmall/mallActivity/page/activity?mallId={}'


def parse_park(response, dic):
    dic['park_rule'] = ''.join(response.xpath('(//div[@class="subTitle"])[1]/text()').getall())
    return dic


def parse_service(response, dic):
    mall_services_json = {}
    dic['mall_hours'] = response.xpath('(//div[@class="card"])[1]//p[@class="mb-flex-1"]/text()').get(default='')
    mall_services_xpath = response.xpath('(//div[@class="card"])[2]//div[@class="type1"]|//div[@class="type2"]')
    dic['all_services_list'] = []

    for mall_service in mall_services_xpath:
        mall_service_name = mall_service.xpath('.//p[@class="mb-flex-1"]/text()').get(default='').strip()
        dic['all_services_list'].append(mall_service_name)
        mall_service_value = mall_service.xpath('.//div[@class="subTitle"]/text()').get(default='').strip()
        if mall_service_value:
            mall_service_name.strip()
            mall_services_json[mall_service_name] = mall_service_value

    dic['mall_services'] = json.dumps(mall_services_json, ensure_ascii=False)
    dic['mall_traffic'] = response.xpath('(//div[@class="card"])[3]//p[@class="mb-flex-1"]/text()').get()
    return dic


def parse_activity(text, dic):
    dic['mall_activity'] = {}
    activity = re.search(r'"6":(.*?)"7"', text, re.S).group(1)
    activity = activity.rstrip().rstrip(',')

    if len(activity):
        activity = json.loads(activity)
        activity = activity.get('list')
        if len(activity):
            activity = activity[0]
            if activity.get('activityUrl'):
                activity['activityUrl'] = unquote(activity['activityUrl'].split('=')[1])
                dic['mall_activity'] = json.dumps(activity, ensure_ascii=False)

    return dic


def get_response(url):
    global headers
    response = requests.get(url=url, headers=headers)
    if response.status_code == 200:
        return response


def get_mall_other_info(city_en_name):
    global base_park_url, base_service_url, base_activity_url
    col = client['DianPing'][f'dp_{city_en_name}_mall']
    mall_ids = [i.get('mall_id') for i in col.find({})]
    x = 0
    for index, mall_id in enumerate(mall_ids[x:]):
        mall_info = dict()
        mall_info['mall_id'] = mall_id

        park_url = base_park_url.format(mall_id)
        service_url = base_service_url.format(mall_id)
        activity_url = base_activity_url.format(mall_id)

        park_resp = get_response(park_url)
        if park_resp:
            park_resp = Selector(response=park_resp)
            mall_info = parse_park(park_resp, mall_info)
        else:
            logger.error(f'{park_url}状态码异常,请处理!!!!!!!!')

        service_resp = get_response(service_url)
        if service_resp:
            service_resp = Selector(response=service_resp)
            mall_info = parse_service(service_resp, mall_info)
        else:
            logger.error(f'{service_url}状态码异常,请处理!!!!!!!!')

        activity_resp = get_response(activity_url)
        if activity_resp:
            mall_info = parse_activity(activity_resp.text, mall_info)
        else:
            logger.error(f'{activity_url}状态码异常,请处理!!!!!!!!')

        print(index+x, mall_info)

        col.update_one({'mall_id': mall_info['mall_id']}, {'$set': mall_info}, upsert=True)


if __name__ == '__main__':
    city_lists = ['广州', '东莞', '佛山', '惠州', '珠海', '中山']
    path1 = 'C:/Users/Administrator/Desktop/dp_city.csv'
    df = pd.read_csv(path1, encoding='gbk')
    df = df.loc[df['name'].isin(city_lists)]
    city_lists = df['en_name'].values.tolist()
    print(city_lists)

    client = pymongo.MongoClient()

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_mall_other_info, city_lists)

    # r = redis.Redis(decode_responses=True)
    client.close()
