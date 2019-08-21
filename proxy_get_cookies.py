import json
import time
import requests
from selenium import webdriver

from proxy_until import create_proxyauth_extension
import redis
import random
import pymongo
import traceback
from selenium.common.exceptions import TimeoutException


class DianPingCookiesPool:

    def __init__(self):
        self.client = pymongo.MongoClient(host='192.168.2.50')
        self.col = self.client['ua']['ua']
        self.r = redis.StrictRedis(decode_responses=True)
        all_city = {
            'guangzhou': '4',
            'dongguan': '219',
            'foshan': '208'
        }
        self.city_id = all_city['dongguan']

        proxy_host = 'http-dyn.abuyun.com'
        proxy_port = '9020'
        proxy_username = 'HAOD93TI7TC5T35D'
        proxy_password = '86C755D8D83FD92B'
        self.proxyauth_plugin_path = create_proxyauth_extension(
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            proxy_username=proxy_username,
            proxy_password=proxy_password,
        )

    @staticmethod
    def get_proxy_adsl(chromeOptions, proxyauth_plugin_path):
        # while True:
        #     proxy = requests.get('http://47.102.147.138:8000/random').text
        #     if proxy:
        #         break
        #     time.sleep(0.01)
        # print(proxy)
        # proxy_host = proxy.split(':')[0]
        # proxy_port = proxy.split(':')[1]
        chromeOptions.add_extension(proxyauth_plugin_path)
        return chromeOptions

    def get_ua_list(self):
        ua_gen = self.col.find({})
        ua_list = [i['ua'] for i in ua_gen]
        return ua_list

    @staticmethod
    def proxy_ua_cookies_json(proxy, ua, cookies_dic):
        all_dic = {}
        if proxy:
            all_dic['proxy'] = proxy
        if ua:
            all_dic['ua'] = ua
        if cookies_dic:
            all_dic['cookies'] = cookies_dic
        all_dic_json = json.dumps(all_dic)
        return all_dic_json

    def get_cookies(self, ua, extension=False, proxy=False):
        chromeOptions = webdriver.ChromeOptions()
        # 开发者选项
        chromeOptions.add_experimental_option('excludeSwitches', ['enable-automation'])
        chromeOptions.add_argument('blink-settings=imagesEnabled=false')
        chromeOptions.add_argument('--disable-gpu')
        if extension:
            chromeOptions = self.get_proxy_adsl(chromeOptions, self.proxyauth_plugin_path)
        if proxy:
            chromeOptions.add_argument(f'--proxy-server=http://{proxy}')
        chromeOptions.add_argument(f'user-agent={ua}')

        browser = webdriver.Chrome(options=chromeOptions)

        browser.set_page_load_timeout(10)

        # browser.get('http://httpbin.org/get')
        try:
            browser.get('https://m.dianping.com')
            browser.delete_all_cookies()
            browser.refresh()
            new_cookies = browser.get_cookies()
        except TimeoutException:
            traceback.print_exc()
            print('代理超时或者进入验证页面')
        else:
            if new_cookies:
                cookies_dic = {}
                for cookie in new_cookies:
                    cookies_dic[cookie['name']] = cookie['value']
                return cookies_dic
            else:
                return None
        finally:
            browser.close()

    def abuyun_main(self):
        self.r.delete('dp_proxy_ua_cookies')
        ua_list = self.get_ua_list()

        for i in range(1500):
            ua = random.choice(ua_list)
            cookies_dic = self.get_cookies(ua, extension=True)
            if cookies_dic:
                cookies_dic['cityid'] = self.city_id
                all_dic_json = self.proxy_ua_cookies_json(None, ua, cookies_dic)
                self.r.sadd('abuyun_cookies', all_dic_json)
                print(all_dic_json)

    def proxy_list_main(self):
        self.r.delete('dp_proxy_ua_cookies')
        ua_list = self.get_ua_list()

        for proxy in self.proxy_list:
            ua = random.choice(ua_list)
            cookies_dic = self.get_cookies(ua, proxy=proxy)
            if cookies_dic:
                cookies_dic['cityid'] = self.city_id
                all_dic_json = self.proxy_ua_cookies_json(proxy, ua, cookies_dic)
                self.r.sadd('dp_proxy_ua_cookies', all_dic_json)
                print(all_dic_json)

    def zhima_main(self):
        self.r.delete('zhima_cookies')
        ua_list = self.get_ua_list()

        ua = random.choice(ua_list)
        cookies_dic = self.get_cookies(ua)
        if cookies_dic:
            # cookies_dic['cityid'] = self.city_id
            cookies_json = json.dumps(cookies_dic)
            self.r.hset('zhima_cookies', ua, cookies_json)


def run():
    cook_pool = DianPingCookiesPool()
    cook_pool.abuyun_main()


if __name__ == '__main__':
    run()



