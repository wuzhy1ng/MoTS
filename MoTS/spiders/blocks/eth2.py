import collections
import json
import logging

import scrapy
from scrapy.utils.misc import load_object

from MoTS import settings
from MoTS.items.block import TransactionMotifItem, InternalTransactionItem
from MoTS.spiders.blocks.eth import BlocksETHSpider
from MoTS.strategies.blocks import BLOCK_MOTIF_COUNTER
from MoTS.tasks.synchronize import SyncMotifCounterTask
from MoTS.utils.enum import ETHDataTypes
from MoTS.utils.url import QueryURLBuilder


class BlocksETH2Spider(BlocksETHSpider):
    name = 'blocks.eth2'
    net = 'eth'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # load apikey bucket class
        apikey_bucket = getattr(settings, 'APIKEYS_BUCKET', None)
        assert apikey_bucket is not None
        self.apikey_bucket = load_object(apikey_bucket)(net='eth', kps=5)

        # api url
        self.base_api_url = 'https://api-cn.etherscan.com/api'

        # transaction motif counter configure
        self.mcounter = kwargs.get('mcounter', None)
        self.motif_size = int(kwargs.get('motif_size', 3))
        self.task_map = dict()
        assert self.mcounter is not None or self.mcounter in BLOCK_MOTIF_COUNTER.keys()

        # task map
        self.task_map = dict()

    def start_requests(self):
        # init motif counter task if available
        for blk in range(self.cur_block, int(self.end_block) + 1):
            task = SyncMotifCounterTask(
                strategy=BLOCK_MOTIF_COUNTER[self.mcounter](
                    motif_size=self.motif_size,
                )
            )
            dtypes = set(self.data_types)
            for _dtypes in [
                {ETHDataTypes.EXTERNAL.value},
                {ETHDataTypes.INTERNAL.value},
                {ETHDataTypes.ERC20.value, ETHDataTypes.ERC721.value, ETHDataTypes.ERC1155.value},
            ]:
                if len(dtypes.intersection(_dtypes)) > 0:
                    task.wait()
            self.task_map[blk] = task

        yield from super(BlocksETH2Spider, self).start_requests()

    async def parse_eth_get_block_by_number(self, response: scrapy.http.Response, **kwargs):
        async for item in self.gen_motif_items(
                super().parse_eth_get_block_by_number(
                    response, **kwargs
                ), **kwargs
        ):
            yield item

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

        async for item in self.gen_motif_items(
                _generator(response, **kwargs),
                **kwargs
        ):
            yield item

    async def parse_eth_get_logs(self, response: scrapy.http.Response, **kwargs):
        items = list()
        async for item in super().parse_eth_get_logs(response, **kwargs):
            items.append(item)

        edges = list()
        for item in items:
            if isinstance(item, scrapy.Item):
                edges.append(item)
            yield item

        # generate motif items
        async for item in self._gen_motif_items(
                task=self.task_map.get(kwargs.get('block_number')),
                edges=edges
        ):
            yield item

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
        )

    async def gen_motif_items(self, generator: collections.Generator, **kwargs):
        edges = list()
        for item in generator:
            if isinstance(item, scrapy.Item):
                edges.append(item)
            yield item

        # generate motif items
        async for item in self._gen_motif_items(
                task=self.task_map.get(kwargs.get('block_number')),
                edges=edges
        ):
            yield item

    @staticmethod
    async def _gen_motif_items(task: SyncMotifCounterTask, edges: [list]):
        if task is None:
            return
        rlt = task.count(edges)
        if rlt is None:
            return
        async for item in rlt:
            print(item)
            yield TransactionMotifItem(
                transaction_hash=item.get('transaction_hash'),
                frequency=item.get('frequency'),
            )
