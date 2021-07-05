# -*- coding: utf-8 -*-
import json
import re
import time
from html import unescape

from scrapy import Spider, Request


def retry_invalid_response(callback):
    def wrapper(spider, response):
        if response.status >= 400:
            if response.status == 404:
                print('Page not 404')
                return

            retry_times = response.meta.get('retry_times', 0)
            if retry_times < 3:
                time.sleep(5)
                response.meta['retry_times'] = retry_times + 1
                return response.request.replace(dont_filter=True, meta=response.meta)

            print("Dropped after 3 retries. url: {}".format(response.url))
            response.meta.pop('retry_times', None)
        return callback(spider, response)

    return wrapper


def clean(text):
    text = re.sub(u'"', u"\u201C", unescape(text or ''))
    text = re.sub(u"'", u"\u2018", text)
    for c in ['\r\n', '\n\r', u'\n', u'\r', u'\t', u'\xa0', '...']:
        text = text.replace(c, ' ')
    return re.sub(' +', ' ', text).strip()


class AliExpressProductsSpider(Spider):
    name = 'aliexpress_products_spider'
    base_url = 'https://www.aliexpress.com/'
    file_name = '../output/products.json'

    start_urls = [
        # men jerseys
        "https://www.aliexpress.com/item/1005002197644774.html?spm=a2g01.12617084.fdpcl001.1.6e4891Lb91LbwW&gps-id=5547572&scm=1007.19201.130907.0&scm_id=1007.19201.130907.0&scm-url=1007.19201.130907.0&pvid=7b32b27f-411c-4ec6-b854-bb261fffe053",
        "https://www.aliexpress.com/item/1005001894096756.html?spm=a2g0o.store_pc_groupList.8148356.37.78a96ac0xKzb2A",
        "https://www.aliexpress.com/item/1005002369171004.html?spm=a2g0o.store_pc_allProduct.8148356.35.23f124c6SFY5le",
        "https://www.aliexpress.com/item/1005002292688624.html?spm=a2g0o.store_pc_allProduct.8148356.39.23f124c68tXL7k",
        "https://www.aliexpress.com/item/1005002304177169.html?spm=a2g0o.store_pc_allProduct.8148356.33.23f124c6h0SFa0",
        "https://www.aliexpress.com/item/1005001593895598.html?spm=a2g0o.store_pc_allProduct.8148356.61.23f124c6eXTcu3",
        # socks
        "https://www.aliexpress.com/item/1005002502030685.html?spm=a2g0o.productlist.0.0.22497306ZskGrG&aem_p4p_detail=20210701054103439204323066160028815822",
        "https://www.aliexpress.com/item/1005001806303442.html?spm=a2g0o.productlist.0.0.22497306ZskGrG&aem_p4p_detail=20210701054103439204323066160028815822",
        "https://www.aliexpress.com/item/1005002502030685.html?spm=a2g0o.productlist.0.0.22497306ZskGrG&aem_p4p_detail=20210701054103439204323066160028815822",
        "https://www.aliexpress.com/item/1005001806303442.html?spm=a2g0o.productlist.0.0.22497306ZskGrG&aem_p4p_detail=20210701054103439204323066160028815822",
        # women jerseys
        "https://www.aliexpress.com/item/1005001608066177.html?spm=a2g0o.productlist.0.0.5916446fLXnQCL&aem_p4p_detail=202107010527337302351380382440028781729",
        "https://www.aliexpress.com/item/1005002330165996.html?spm=a2g0o.detail.1000060.2.40f889ablQTjGV&gps-id=pcDetailBottomMoreThisSeller&scm=1007.13339.169870.0&scm_id=1007.13339.169870.0&scm-url=1007.13339.169870.0&pvid=6a46cf15-65ff-48fc-92b5-efc025891c12&_t=gps-id:pcDetailBottomMoreThisSeller,scm-url:1007.13339.169870.0,pvid:6a46cf15-65ff-48fc-92b5-efc025891c12,tpp_buckets:668%230%23208986%2313_668%230%23208986%2313_668%23888%233325%2311_668%23888%233325%2311_668%232846%238115%232000_668%235811%2327169%235_668%232717%237567%23906_668%231000022185%231000066058%230_668%233468%2315607%2345_668%232846%238115%232000_668%235811%2327169%235_668%232717%237567%23906_668%233164%239976%23638_668%233468%2315607%2345",
        # men shorts
        "https://www.aliexpress.com/item/32825218824.html?spm=a2g0o.detail.1000060.3.1f286eafd4XKHM&gps-id=pcDetailBottomMoreThisSeller&scm=1007.13339.169870.0&scm_id=1007.13339.169870.0&scm-url=1007.13339.169870.0&pvid=f5b83198-fbe1-4c33-b8cb-c169ec381c69&_t=gps-id:pcDetailBottomMoreThisSeller,scm-url:1007.13339.169870.0,pvid:f5b83198-fbe1-4c33-b8cb-c169ec381c69,tpp_buckets:668%230%23208986%2318_668%230%23208986%2318_668%23888%233325%2311_668%23888%233325%2311_668%232846%238115%232000_668%235811%2327169%235_668%232717%237567%23906_668%231000022185%231000066058%230_668%233468%2315612%23370_668%232846%238115%232000_668%235811%2327169%235_668%232717%237567%23906_668%233164%239976%23638_668%233468%2315612%23370",
        "https://www.aliexpress.com/item/32825218824.html?spm=a2g0o.detail.1000060.3.1f286eafd4XKHM&gps-id=pcDetailBottomMoreThisSeller&scm=1007.13339.169870.0&scm_id=1007.13339.169870.0&scm-url=1007.13339.169870.0&pvid=f5b83198-fbe1-4c33-b8cb-c169ec381c69&_t=gps-id:pcDetailBottomMoreThisSeller,scm-url:1007.13339.169870.0,pvid:f5b83198-fbe1-4c33-b8cb-c169ec381c69,tpp_buckets:668%230%23208986%2318_668%230%23208986%2318_668%23888%233325%2311_668%23888%233325%2311_668%232846%238115%232000_668%235811%2327169%235_668%232717%237567%23906_668%231000022185%231000066058%230_668%233468%2315612%23370_668%232846%238115%232000_668%235811%2327169%235_668%232717%237567%23906_668%233164%239976%23638_668%233468%2315612%23370",
        # men_cycling_shoes
        "https://www.aliexpress.com/item/1005001931335793.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-8",
        "https://www.aliexpress.com/item/1005002359199946.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-12",
        "https://www.aliexpress.com/item/1005002022115720.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-6",
        "https://www.aliexpress.com/item/4001362307724.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-28",
        "https://www.aliexpress.com/item/4000447382291.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-31",
        "https://www.aliexpress.com/item/1005001931335793.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-8",
        "https://www.aliexpress.com/item/1005002359199946.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-12",
        "https://www.aliexpress.com/item/1005002022115720.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-6",
        "https://www.aliexpress.com/item/4001362307724.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-28",
        "https://www.aliexpress.com/item/4000447382291.html?spm=a2g0o.productlist.0.0.7742578fzLj1aZ&algo_pvid=5883965c-9c86-4c4c-91b5-cae5f8ccc249&algo_exp_id=5883965c-9c86-4c4c-91b5-cae5f8ccc249-31"

    ]

    handle_httpstatus_list = [
        400, 401, 402, 403, 404, 405, 406, 407, 409,
        500, 501, 502, 503, 504, 505, 506, 507, 509,
    ]

    cols = [
        "Title", "Categories", "Breadcrum", "Sale Price", "Regular Price", "Image URLs", "SKU"
    ]

    output_file = open(file_name, mode='w')
    output_file.write('[' + '\n')

    meta = {
        'handle_httpstatus_list': handle_httpstatus_list,
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,'
                  'Application/signed-exchange;v=b3;q=0.9',
        'Accept-encoding': 'gzip, deflate, br',
        'Accept-language': 'en-US,en;q=0.9',
        'Cache-control': 'no-cache',
        'Pragma': 'no-cache',
        'Sec-fetch-mode': 'navigate',
        'Sec-fetch-site': 'none',
        'Sec-fetch-user': '?1',
        'Upgrade-insecure-requests': 1,
        'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/79.0.3945.130 Safari/537.36'
    }

    def __init__(self, **kwargs):
        super(AliExpressProductsSpider, self).__init__(**kwargs)

    def start_requests(self):
        for url in self.start_urls[:]:
            yield Request(url, callback=self.parse_details,
                          headers=self.headers, errback=self.handle_error, meta=self.meta)

    @retry_invalid_response
    def parse_details(self, response):
        raw = json.loads(re.findall("data:(.*),", response.text)[0].strip())
        item = dict()
        item['Title'] = self.get_title(raw)
        item['Image URLs'] = self.get_images(raw)
        item['Breadcrum'] = self.get_breadcrums(raw)
        item['Sale Price'] = raw['priceModule']['minAmount']['value']
        item['Regular Price'] = raw['priceModule']['maxAmount']['value']
        item['SKU'] = self.get_skus(raw)
        item['Categories'] = self.get_breadcrums(raw)[-1]

        response.meta['item'] = item
        return Request(url=raw['descriptionModule']['descriptionUrl'], callback=self.parse_description,
                       headers=self.headers, errback=self.handle_error, meta=response.meta)

    def parse_description(self, response):
        item = response.meta['item']
        item['Description'] = self.get_description(response)
        self.write_to_file(item)
        return item

    def handle_error(self, error):
        time.sleep(5)
        req = error.request
        retry_times = req.meta.get('retry_times', 0)
        if retry_times < 3:
            priority = req.priority - 5
            req.meta['retry_times'] = retry_times + 1
            return req.replace(priority=priority)
        self.logger.debug("After 3 retries, Item is drpped due to {}".format(error))

    def get_title(self, raw):
        return raw['titleModule']['subject']

    def get_images(self, raw):
        return [img.replace('50x50', '480x480') for img in
                raw['imageModule']['summImagePathList'] or []]

    def get_breadcrums(self, raw):
        return [e['name'] for e in raw['crossLinkModule']['breadCrumbPathList']]

    def get_skus(self, raw):
        skus = []
        colors = {}
        sizes = {}

        for sk in raw['skuModule']['productSKUPropertyList']:
            if sk['skuPropertyName'] == 'Color':
                for color in sk['skuPropertyValues']:
                    colors[str(color['propertyValueId'])] = {
                        'color_name': color['propertyValueName'],
                        'color_image_url': color['skuPropertyImagePath'],
                    }

            elif sk['skuPropertyName'] in ['Size', 'Shoe Size']:
                for size in sk['skuPropertyValues']:
                    sizes[str(size['propertyValueId'])] = size['propertyValueName']

        for sp in raw['skuModule']['skuPriceList']:
            sku_ids = sp['skuPropIds'].split(',')
            skus.append({
                'sku_id': sp['skuId'],
                'sale_price': sp['skuVal']['actSkuCalPrice'],
                'regular_price': sp['skuVal']['skuCalPrice'],
                'size': sizes[sku_ids[1].strip()] if sizes else "No Size",
                **colors[sku_ids[0].strip()],
            })

        return skus

    def get_description(self, response):
        desc = []
        for sel in response.css('p, span'):
            desc += sel.css('::Text').getall()
            desc += sel.css('::attr(src)').getall()

        return "\n".join(clean(e) for e in desc if clean(e))

    def write_to_file(self, item):
        json.dump(item, self.output_file, indent=2)
        self.output_file.write(', \n')

    def close(spider, reason):
        spider.output_file.write(']')
        spider.output_file.close()
