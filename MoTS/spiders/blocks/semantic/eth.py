import collections
import json
from collections import AsyncGenerator, Iterator

import scrapy

from MotTS.items.block import TransactionMotifItem, ExternalTransactionItem, \
    InternalTransactionItem, ERC20TokenTransferItem, ERC721TokenTransferItem, ERC1155TokenTransferItem
from MotTS.spiders.blocks.eth import BlocksETHSpider
from MotTS.strategies.blocks import BLOCK_MOTIF_COUNTER
from MotTS.tasks.synchronize import SyncMotifCounterTask
from MotTS.utils.enum import ETHDataTypes


class BlocksSemanticETHSpider(BlocksETHSpider):
    name = 'blocks.semantic.eth'
    net = 'eth'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # transaction motif counter configure
        self.mcounter = kwargs.get('mcounter', 'highorder')
        self.motif_size = int(kwargs.get('motif_size', 4))
        self.task_map = dict()
        assert self.mcounter is not None or self.mcounter in BLOCK_MOTIF_COUNTER.keys()

        # task map
        self.task_map = dict()

    def init_tasks(self, cur_block, end_block):
        dtypes = set(self.data_types)
        for blk in range(cur_block, int(end_block) + 1):
            task = SyncMotifCounterTask(
                strategy=BLOCK_MOTIF_COUNTER[self.mcounter](
                    motif_size=self.motif_size,
                )
            )
            for _dtypes in [
                {ETHDataTypes.EXTERNAL.value},
                {ETHDataTypes.INTERNAL.value},
                {ETHDataTypes.ERC20.value, ETHDataTypes.ERC721.value, ETHDataTypes.ERC1155.value},
            ]:
                if len(dtypes.intersection(_dtypes)) > 0:
                    task.wait()
            self.task_map[blk] = task

    def start_requests(self):
        if self.end_block is None:
            yield self.get_request_eth_block_number()
            return

        # init motif counter task if available
        self.init_tasks(self.cur_block, self.end_block)
        yield from super(BlocksSemanticETHSpider, self).start_requests()

    def parse_eth_block_number(self, response: scrapy.http.Response, **kwargs):
        result = json.loads(response.text)
        result = result.get('result')
        blk = int(result, 16)
        if blk > self.cur_block:
            self.init_tasks(self.cur_block, blk)

        super(BlocksSemanticETHSpider, self).parse_eth_block_number(response, **kwargs)

    async def parse_eth_get_block_by_number(self, response: scrapy.http.Response, **kwargs):
        edges = list()
        async for item in super(BlocksSemanticETHSpider, self).parse_eth_get_block_by_number(
                response, **kwargs
        ):
            if self._is_transfer(item):
                edges.append(item)
            yield item

        # generate motif items
        async for item in self._gen_motif_items(
                task_key=kwargs.get('block_number'),
                edges=edges
        ):
            yield item

    async def parse_trace_block(self, response: scrapy.http.Response, **kwargs):
        async for item in self.gen_motif_items(
                super(BlocksSemanticETHSpider, self).parse_trace_block(
                    response, **kwargs
                ), **kwargs
        ):
            yield item

    async def parse_eth_get_logs(self, response: scrapy.http.Response, **kwargs):
        edges = list()
        async for item in super(BlocksSemanticETHSpider, self).parse_eth_get_logs(response, **kwargs):
            if self._is_transfer(item):
                edges.append(item)
            yield item

        # generate motif items
        async for item in self._gen_motif_items(
                task_key=kwargs.get('block_number'),
                edges=edges
        ):
            yield item

    async def handle_request_error(self, failure):
        for item in super(BlocksSemanticETHSpider, self).handle_request_error(failure):
            yield item

        # generate motif items if the request failed
        kwargs = failure.request.cb_kwargs
        async for item in self._gen_motif_items(
                task_key=kwargs.get('block_number'),
                edges=list()
        ):
            yield item

    async def gen_motif_items(self, generator: collections.Generator, **kwargs):
        edges = list()
        for item in generator:
            if self._is_transfer(item):
                edges.append(item)
            yield item

        # generate motif items
        async for item in self._gen_motif_items(
                task_key=kwargs.get('block_number'),
                edges=edges
        ):
            yield item

    async def _gen_motif_items(self, task_key: str, edges: [list]):
        task = self.task_map.get(task_key)
        if task is None:
            return
        rlt = task.count(edges)
        if rlt is None:
            return

        # delete task and generate semantic item
        self.task_map.pop(task_key)
        if isinstance(rlt, AsyncGenerator):
            async for item in rlt:
                yield TransactionMotifItem(
                    transaction_hash=item.get('transaction_hash'),
                    frequency=item.get('frequency'),
                )
        if isinstance(rlt, Iterator):
            for item in rlt:
                yield TransactionMotifItem(
                    transaction_hash=item.get('transaction_hash'),
                    frequency=item.get('frequency'),
                )

    @staticmethod
    def _is_transfer(item) -> bool:
        return any([
            isinstance(item, ExternalTransactionItem),
            isinstance(item, InternalTransactionItem),
            isinstance(item, ERC20TokenTransferItem),
            isinstance(item, ERC721TokenTransferItem),
            isinstance(item, ERC1155TokenTransferItem),
        ])
