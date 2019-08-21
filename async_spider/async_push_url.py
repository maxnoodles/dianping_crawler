import asyncio
import random
import time

import aiohttp
from fake_useragent import UserAgent
from motor.motor_asyncio import AsyncIOMotorClient
import aioredis
import requests


class AddUrl:
    def __init__(self):
        self.client = AsyncIOMotorClient(host='127.0.0.1', port=27017)
        self.col_read = self.client['DianPing']['dp_shop_error']
        self.set_name = 'dp_shop'

    async def start(self):
        self.rd = await aioredis.create_redis_pool(('localhost', 6379), encoding='utf-8')
        cursor = self.col_read.find({})
        tasks = [asyncio.create_task(self.add(shop['url'])) for shop in await cursor.to_list(length=None)]
        # tasks = [asyncio.create_task(self.get_all())]
        await asyncio.wait(tasks)

        result = [task.result() for task in tasks]
        print(len(result))
        self.rd.close()
        await self.rd.wait_closed()
        self.client.close()

    async def add(self, url):
        await self.rd.sadd(self.set_name, url)

    async def get_all(self):
        result = await self.rd.smembers(self.set_name)
        return result


def run():
    add_url = AddUrl()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(add_url.start())
    loop.close()


if __name__ == '__main__':
    run()