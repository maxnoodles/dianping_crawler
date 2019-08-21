"# dianping_crawler" 

`dp_mall_info.py` 通过点评商场接口抓取商场基本信息  

`dp_mall_other_info.py` 通过点评app接口抓取商城其他信息(停车，活动等)  

`dp_shop.py` 通过点评店铺接口抓取店铺基本信息  

`dp_shop_detail.py` 通过手机端点评店铺详情页抓取店铺详情信息  

`proxy_until.py` 通过 selenium 添加谷歌代理扩展，主要是让 selenium 可以使用带认证的代理  

`proxy_get_ccookies.py` 通过 selenium 获取点评首页的cookies，序列化后存入 redis  


`async_spider` 异步版本  
