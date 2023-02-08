import hashlib
import json

import scrapy

from MoTS.items import LabelItem


class ActionSpider(scrapy.Spider):
    name = 'labels.action'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # output path
        self.out_dir = kwargs.get('out', './data')

        # tx block range
        self.start_blk = int(kwargs.get('start_blk', 0))
        self.end_blk = int(kwargs.get('end_blk', 99999999))

    def start_requests(self):
        for blk in range(self.start_blk, self.end_blk + 1, 20):
            yield self.get_block_request(
                block=blk,
                page=1,
                priority=self.end_blk - blk,
                has_next=True,
            )

    def parse_txs(self, response: scrapy.http.Response, **kwargs):
        for tr in response.xpath('//*[@id="paywall_mask"]/table/tbody/tr'):
            txhash = tr.xpath('./td[2]//a/text()').get()
            method = tr.xpath('./td[3]/span/text()').get()
            if txhash is None or method is None:
                continue
            info = {
                'txhash': txhash,
                'action': method,
                'url': response.url,
            }
            yield LabelItem(
                _id=hashlib.sha256(json.dumps(info).encode()).hexdigest(),
                net='eth',
                labels=[method],
                category='transaction',
                info=info,
            )

        if not kwargs.get('has_next'):
            return

        max_page = response.xpath(
            '//div[@id="ContentPlaceHolder1_topPageDiv"]/nav//span'
            '[@class="page-link text-nowrap"]/strong[2]/text()'
        ).get()
        if max_page is not None and int(max_page) > 1:
            for i in range(2, int(max_page) + 1):
                yield self.get_block_request(
                    block=kwargs['block'],
                    page=i,
                    priority=response.request.priority
                )

    def get_block_request(self, block: int, page: int, priority: int, **kwargs):
        return scrapy.Request(
            url='https://cn.etherscan.com/txs?block={}&ps=100&p={}'.format(
                block, page
            ),
            method='GET',
            priority=priority,
            cb_kwargs={'block': block, 'page': page, 'priority': priority, **kwargs},
            callback=self.parse_txs,
            dont_filter=True,
        )
