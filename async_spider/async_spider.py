import asyncio
import json
import random
import time

import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
import aioredis
from itertools import islice
from aiohttp.client_exceptions import ClientConnectionError, ServerTimeoutError, ClientConnectorSSLError
from asyncio import TimeoutError
import traceback
from ssl import SSLError


class DianPingSpider:

    def __init__(self):
        self.headers = {
                'Connection': 'keep-alive',
                'Cache-Control': 'max-age=0',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,'
                          'image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9',
            }

        # self.proxy = ''
        self.client = AsyncIOMotorClient()
        self.col = self.client['ua']['ua']
        self.proxy_url = 'http://47.102.147.138:8000/random'
        self.city_en_name_list = ['dongguan', 'foshan', 'guangzhou']
        self.base_set_name = 'dp_{}_shops'
        self.proxy_hash_name = 'useful_proxy'
        self.ua_cookies_hash = 'dp_cookies'

    async def create_redis(self):
        self.r = await aioredis.create_redis_pool(('localhost', 6379), encoding='utf-8')
        self.ua_list = await self.r.hkeys(self.ua_cookies_hash)

    async def start_request(self):
        """
        包装 url 使用协程抓取
        :return:
        """
        await self.create_redis()
        city_en_name = self.city_en_name_list[2]
        set_name = self.base_set_name.format(city_en_name)

        url_lists = await self.r.smembers(set_name)
        async with aiohttp.TCPConnector(limit=300, force_close=True, enable_cleanup_closed=True) as tc:
            async with aiohttp.ClientSession(connector=tc) as session:
                tasks = [asyncio.create_task(self.fetch(url, session, city_en_name)) for url in url_lists]
                await asyncio.wait(tasks)
                self.r.close()
                await self.r.wait_closed()
                # tasks = (asyncio.create_task(self.fetch(url, session, city_en_name)) for url in url_lists)
                # await self.branch(tasks)
    #
    # async def branch(self, tasks, limit=900):
    #     """切片，限制并发"""
    #     index = 0
    #     while True:
    #         slice_gen = islice(tasks, index, limit)
    #         slice_list = list(slice_gen)
    #         if not slice_list:
    #             break
    #         await asyncio.create_task(asyncio.wait(slice_list))
    #     self.r.close()
    #     await self.r.wait_closed()

    def get_ua(self):
        ua = random.choice(self.ua_list)
        return ua

    async def get_proxy(self, session):
        # 使用代理
        while True:
            async with session.get(self.proxy_url) as res:
                proxy_text = await res.text()
                if proxy_text:
                    break
                await asyncio.sleep(0.1)
        proxy = 'http://' + proxy_text
        return proxy

    async def get_cookies(self, ua):
        # while True:
        #     ua_list = await self.r.hkeys(self.ua_cookies_hash)
        #     if ua_list and len(ua_list):
        #         ua = random.choice(ua_list)
        #         cookies = await self.r.hget(self.ua_cookies_hash, ua)
        #         cookies = json.loads(cookies)
        #         break
        #     print('等待cookies')
        #     await asyncio.sleep(1)
        cookies = await self.r.hget(self.ua_cookies_hash, ua)
        return cookies

    async def get_proxy_ua_cookies(self):
        info_json = await self.r.srandmember('dp_proxy_ua_cookies')
        info_dic = json.loads(info_json)
        proxy = info_dic['proxy']
        ua = info_dic['ua']
        cookies = info_dic['cookies']
        return proxy, ua, cookies, info_json

    async def fetch(self, url, session, city_en_name):
        """
        讲返回的内容不做解析直接入库, 记得 json.loads(cookies)!!!
        :param url:
        :param session:
        :param city_en_name:
        :return:
        """
        if await self.r.sismember('dp_finish', url):
            print(f'{url}已经抓取过，跳过')
            return

        # ua = self.get_ua()
        # cookies = self.get_cookies(ua)

        # ua = await self.r.hkeys('zhima_cookies')
        # ua = ua[0]
        # cookies = await self.r.hget('zhima_cookies', ua)
        proxy_flag = False

        if proxy_flag:
            proxy = self.get_proxy(session)
            proxy_auth = aiohttp.BasicAuth('d88', 'd88')
        else:
            proxy = None
            proxy_auth = None

        proxy, ua, cookies, info_json = await self.get_proxy_ua_cookies()
        proxy = 'http://' + proxy
        # cookies = json.loads(cookies)
        self.headers['user-agent'] = ua

        try:
            async with session.get(url,
                                   headers=self.headers,
                                   proxy=proxy,
                                   proxy_auth=proxy_auth,
                                   cookies=cookies,
                                   timeout=10,
                                   ssl=False) as res:
                if res.status == 200 and 'verify' not in str(res.url):
                    res_text = await res.text()
                    # 异步插入redis
                    print('获取成功', res.url)
                    await self.r.hset(f'dp_{city_en_name}_text', url, res_text)
                    await self.r.sadd('dp_finish', url)
                else:
                    # await self.r.hdel(self.ua_cookies_hash, ua)
                    print(f'状态码错误或者进入验证中心:{res.status}', url)
                    # await self.r.srem('dp_proxy_ua_cookies', info_json)
        # except [ClientConnectionError, ServerTimeoutError, SSLError, TypeError, TimeoutError]:
        except Exception as e:
            # pass
            # traceback.print_exc()
            print(e)


def run():
    """不能使用 asyncio.run()，和 motor 冲突"""
    print('开始抓取点评数据')
    spider = DianPingSpider()
    now = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(spider.start_request())
    loop.run_until_complete(asyncio.sleep(0.250))
    # loop.close()
    times = time.time() - now
    print('抓取点评数据耗时: ', times)


if __name__ == '__main__':
    run()
