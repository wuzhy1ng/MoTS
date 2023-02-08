import json
import logging

import scrapy
from scrapy.utils.misc import load_object

from MotTS import settings
from MotTS.items import InternalTransactionItem, BlockNumberItem
from MotTS.spiders.blocks.semantic.eth import BlocksSemanticETHSpider
from MotTS.utils.url import QueryURLBuilder


class BlocksSemanticETH2Spider(BlocksSemanticETHSpider):
    name = 'blocks.semantic.eth2'
    net = 'eth'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # load apikey bucket class
        apikey_bucket = getattr(settings, 'APIKEYS_BUCKET', None)
        assert apikey_bucket is not None
        self.apikey_bucket = load_object(apikey_bucket)(net='eth', kps=5)

        # api url
        self.base_api_url = 'https://api-cn.etherscan.com/api'

    async def parse_trace_block(self, response: scrapy.http.Response, **kwargs):
        def _generator(response: scrapy.http.Response, **kwargs):
            data = json.loads(response.text)
            data = data.get('result')
            if not isinstance(data, list):
                self.log(
                    message="Error on parsing external block: %s" % str(data),
                    level=logging.ERROR
                )
                return

            for tx in data:
                yield InternalTransactionItem(
                    transaction_hash=tx.get('hash', ''),
                    transaction_position=-1,
                    trace_type=tx.get('type', ''),
                    block_number=tx.get('blockNumber', -1),
                    address_from=tx.get('from', ''),
                    address_to=tx.get('to', ''),
                    value=int(tx.get('value', -1)),
                    gas=int(tx.get('gas', -1)),
                    gas_used=int(tx.get('gasUsed', -1)),
                    timestamp=int(tx.get('timeStamp', -1)),
                    input=tx.get('input', ''),
                    output='',
                )

        try:
            async for item in self.gen_motif_items(
                    _generator(response, **kwargs),
                    **kwargs
            ):
                yield item
        finally:
            # generate block number for recording crawl progress
            latest_blk = self._block_recorder.record(kwargs['block_number'])
            if latest_blk is not None:
                yield BlockNumberItem(block_number=latest_blk)

    def get_request_trace_block(self, block_number: int, priority: int, cb_kwargs: dict) -> scrapy.Request:
        url = QueryURLBuilder(self.base_api_url).get({
            'module': 'account',
            'action': 'txlistinternal',
            'startblock': block_number,
            'endblock': block_number,
            'apikey': self.apikey_bucket.get()
        })
        return scrapy.Request(
            url=url,
            method='GET',
            priority=priority,
            cb_kwargs=cb_kwargs,
            callback=self.parse_trace_block,
            errback=self.handle_request_error,
        )
