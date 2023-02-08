import asyncio
import json
import logging
import time
from typing import Union

import aiohttp
import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.misc import load_object
from twisted.internet.error import TCPTimedOutError, DNSLookupError
from web3 import Web3

from MoTS import settings
from MoTS.items import BlockMetaItem, ExternalTransactionItem, InternalTransactionItem
from MoTS.items.block import ERCTokenItem, ERC20TokenTransferItem, ERC721TokenTransferItem, \
    ERC1155TokenTransferItem, BlockNumberItem, LogItem
from MoTS.utils.enum import ETHDataTypes


class BlocksETHSpider(scrapy.Spider):
    name = 'blocks.eth'
    net = 'eth'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # output dir and block range
        self.out_dir = kwargs.get('out', './data')
        self.cur_block = int(kwargs.get('start_blk', '0'))
        self.end_block = kwargs.get('end_blk', None)

        # extract data types
        self.data_types = kwargs.get('types', 'meta,external').split(',')
        for data_type in self.data_types:
            assert ETHDataTypes.has(data_type)

        # provider settings
        provider_bucket = getattr(settings, 'PROVIDERS_BUCKET', None)
        assert provider_bucket is not None
        self.provider_bucket = load_object(provider_bucket)(net='eth', kps=10000)

        # aiohttp concurrent configuration
        self.aiohttp_lock = asyncio.Semaphore(getattr(settings, 'CONCURRENT_REQUESTS', 16))

        # web3 object
        self._web3 = Web3(provider=Web3.HTTPProvider(self.provider_bucket.get()))

        # token transfer event topic
        self.ERC20_TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        self.ERC721_TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        self.ERC1155_SINGLE_TRANSFER_TOPIC = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
        self.ERC1155_BATCH_TRANSFER_TOPIC = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'

        # token info cache
        self._cache_contract_info = dict()

        # retry
        self.max_retry = int(kwargs.get('retry', 3))

        # crawl block records
        self._block_recorder = BlockRecorder(self.cur_block)

    def start_requests(self):
        if self.end_block is None:
            yield self.get_request_eth_block_number()
            return
        self.end_block = int(self.end_block)

        # init recoder
        for blk in range(self.cur_block, self.end_block + 1):
            self._block_recorder.wait(blk)

        # generate requests
        yield from self.gen_requests(self.cur_block, self.end_block)

    def parse_eth_block_number(self, response: scrapy.http.Response, **kwargs):
        func_name = 'parse_eth_block_number'
        self.log(message='On {}, with {}'.format(func_name, str(kwargs)), level=logging.DEBUG)
        result = json.loads(response.text)
        result = result.get('result')

        # generate more requests
        if result is not None:
            blk = int(result, 16)
            if blk > self.cur_block:
                # init recoder
                for _blk in range(self.cur_block, blk + 1):
                    self._block_recorder.wait(_blk)

                # generate requests
                yield from self.gen_requests(self.cur_block, blk)
                self.cur_block = blk
        else:
            self.log(
                message="Result field is None on {} with {}, ".format(func_name, kwargs) +
                        "please ensure that whether the provider is available.",
                level=logging.ERROR
            )

        # next query of block number
        self.log(
            message="Query the latest block number after 5 seconds...",
            level=logging.INFO
        )
        time.sleep(5)
        yield self.get_request_eth_block_number()

    async def parse_eth_get_block_by_number(self, response: scrapy.http.Response, **kwargs):
        async def _parse_eth_get_block_by_number(response: scrapy.http.Response, **kwargs):
            func_name = 'parse_eth_get_block_by_number'
            self.log(message='On {}, with {}'.format(func_name, str(kwargs)), level=logging.DEBUG)
            result = json.loads(response.text)
            result = result.get('result')
            if result is None:
                yield self.handle_response_error(
                    response=response,
                    func_name=func_name,
                    cb_kwargs=kwargs
                )
                return

            # generate block meta item
            if ETHDataTypes.META.value in self.data_types:
                yield BlockMetaItem(
                    block_hash=result.get('hash', ''),
                    block_number=self.hex_to_dec(result.get('number')),
                    parent_hash=result.get('parentHash', ''),
                    difficulty=self.hex_to_dec(result.get('difficulty')),
                    total_difficulty=self.hex_to_dec(result.get('totalDifficulty')),
                    size=self.hex_to_dec(result.get('size')),
                    transaction_count=len(result.get('transactions', list())),
                    gas_limit=self.hex_to_dec(result.get('gasLimit')),
                    gas_used=self.hex_to_dec(result.get('gasUsed')),
                    miner=result.get('miner', ''),
                    receipts_root=result.get('receiptsRoot', ''),
                    timestamp=self.hex_to_dec(result.get('timestamp')),
                    logs_bloom=result.get('logsBloom', ''),
                    nonce=self.hex_to_dec(result.get('nonce')),
                )

            # fetch transactions
            txs = result.get('transactions')
            assert isinstance(txs, list)

            # extract external transactions of block
            has_internal_txs = False
            for tx in txs:
                if result.get('input') != '0x':
                    has_internal_txs = True

                if ETHDataTypes.EXTERNAL.value in self.data_types:
                    item = ExternalTransactionItem(
                        transaction_hash=tx.get('hash', ''),
                        transaction_index=self.hex_to_dec(tx.get('transactionIndex')),
                        block_hash=tx.get('blockHash', ''),
                        block_number=self.hex_to_dec(tx.get('blockNumber')),
                        address_from=tx['from'] if tx.get('from') else '',
                        address_to=tx['to'] if tx.get('to') else '',
                        is_create_contract=False,
                        value=self.hex_to_dec(tx.get('value')),
                        gas=self.hex_to_dec(tx.get('gas')),
                        gas_price=self.hex_to_dec(tx.get('gasPrice')),
                        timestamp=self.hex_to_dec(result.get('timestamp')),
                        nonce=self.hex_to_dec(tx.get('nonce')),
                        input=tx.get('input', ''),
                    )
                    # call `eth_getTransactionReceipt` if contract created
                    if len(item['address_to']) == 0:
                        item['address_to'] = await self.get_created_contract_address(item['transaction_hash'])
                        item['is_create_contract'] = True
                    yield item

            # fetch internal transaction items
            if has_internal_txs and ETHDataTypes.INTERNAL.value in self.data_types:
                self._block_recorder.wait(kwargs['block_number'])
                yield self.get_request_trace_block(
                    block_number=kwargs['block_number'],
                    priority=kwargs['priority'],
                    cb_kwargs={
                        'block_number': kwargs['block_number'],
                        'timestamp': self.hex_to_dec(result.get('timestamp')),
                        'priority': kwargs['priority'],
                    }
                )

            # fetch log items
            if has_internal_txs and any([
                ETHDataTypes.TOKEN.value in self.data_types,
                ETHDataTypes.ERC20.value in self.data_types,
                ETHDataTypes.ERC721.value in self.data_types,
                ETHDataTypes.ERC1155.value in self.data_types,
                ETHDataTypes.LOG.value in self.data_types,
            ]):
                self._block_recorder.wait(kwargs['block_number'])
                yield self.get_request_eth_get_logs(
                    block_number=kwargs['block_number'],
                    priority=kwargs['priority'],
                    cb_kwargs={
                        'block_number': kwargs['block_number'],
                        'timestamp': self.hex_to_dec(result.get('timestamp')),
                        'priority': kwargs['priority'],
                    }
                )

        # generate block number for recording crawl progress
        try:
            async for item in _parse_eth_get_block_by_number(response, **kwargs):
                yield item
        finally:
            latest_blk = self._block_recorder.record(kwargs['block_number'])
            if latest_blk is not None:
                yield BlockNumberItem(block_number=latest_blk)

    def parse_trace_block(self, response: scrapy.http.Response, **kwargs):
        def _parse_trace_block(response: scrapy.http.Response, **kwargs):
            func_name = 'parse_trace_block'
            self.log(message='On {}, with {}'.format(func_name, str(kwargs)), level=logging.DEBUG)
            result = json.loads(response.text)
            result = result.get('result')
            if result is None:
                yield self.handle_response_error(
                    response=response,
                    func_name=func_name,
                    cb_kwargs=kwargs
                )
                return

            # extract internal transactions of block
            for trace in result:
                action = trace.get('action')
                trace_result = trace.get('result')
                if trace.get('transactionHash') is None:
                    continue
                yield InternalTransactionItem(
                    transaction_hash=trace.get('transactionHash', ''),
                    transaction_position=trace.get('transactionPosition', -1),
                    trace_type=trace.get('type', ''),
                    trace_address=trace.get('traceAddress', list()),
                    subtraces=trace.get('subtraces', -1),
                    block_number=trace.get('blockNumber', -1),
                    address_from=action.get('from', '') if action else '',
                    address_to=action.get('to', '') if action else '',
                    value=self.hex_to_dec(action.get('value')) if action else -1,
                    gas=self.hex_to_dec(action.get('gas')) if action else -1,
                    gas_used=self.hex_to_dec(trace_result.get('gasUsed')) if trace_result else -1,
                    timestamp=kwargs['timestamp'],
                    input=action.get('input', '') if action else '',
                    output=trace_result.get('output', '') if trace_result else '',
                )

        # generate block number for recording crawl progress
        try:
            yield from _parse_trace_block(response, **kwargs)
        finally:
            latest_blk = self._block_recorder.record(kwargs['block_number'])
            if latest_blk is not None:
                yield BlockNumberItem(block_number=latest_blk)

    async def parse_eth_get_logs(self, response: scrapy.http.Response, **kwargs):
        def _generate_block_logs(logs: list, **kwargs):
            if ETHDataTypes.LOG.value not in self.data_types:
                return

            for log in logs:
                yield LogItem(
                    transaction_hash=log.get('transactionHash', ''),
                    log_index=self.hex_to_dec(log.get('logIndex')),
                    block_number=self.hex_to_dec(log.get('blockNumber')),
                    timestamp=kwargs['timestamp'],
                    address=log.get('address', ''),
                    topics=log.get('topics', list()),
                    data=log.get('data', ''),
                    removed=log.get('removed', False),
                )

        async def _generate_block_erc20_transfers(logs: list, **kwargs):
            if ETHDataTypes.ERC20.value not in self.data_types:
                return

            for log in logs:
                topics = log.get('topics')
                if any([
                    topics is None,
                    len(topics) < 1,
                    topics[0] != self.ERC20_TRANSFER_TOPIC
                ]):
                    continue

                # load log data
                data = log.get('data')
                topics_with_data = topics + self.split_to_words(data)
                if len(topics_with_data) != 4:
                    continue

                # load token info
                contract_address = log.get('address', '').lower()
                if self._cache_contract_info.get(contract_address) is None:
                    info = await self.get_erc20_or_erc721_info(contract_address=contract_address)
                    self._cache_contract_info[contract_address] = info
                    if ETHDataTypes.TOKEN.value in self.data_types:
                        yield ERCTokenItem(**info)
                contract_info = self._cache_contract_info[contract_address]

                # extract erc20 transfer
                if not contract_info['is_erc20']:
                    continue
                yield ERC20TokenTransferItem(
                    transaction_hash=log.get('transactionHash', ''),
                    log_index=self.hex_to_dec(log.get('logIndex')),
                    block_number=self.hex_to_dec(log.get('blockNumber')),
                    timestamp=kwargs['timestamp'],
                    address_from=self.word_to_address(topics_with_data[1]),
                    address_to=self.word_to_address(topics_with_data[2]),
                    value=self.hex_to_dec(topics_with_data[3]),
                    contract_address=contract_address,
                    token_symbol=contract_info.get('token_symbol'),
                    decimals=contract_info.get('decimals'),
                    total_supply=contract_info.get('total_supply'),
                )

        async def _generate_block_erc721_transfers(logs: list, **kwargs):
            if ETHDataTypes.ERC721.value not in self.data_types:
                return

            for log in logs:
                topics = log.get('topics')
                if any([
                    topics is None,
                    len(topics) < 1,
                    topics[0] != self.ERC20_TRANSFER_TOPIC
                ]):
                    continue

                # load log data
                data = log.get('data')
                topics_with_data = topics + self.split_to_words(data)
                if len(topics_with_data) != 4:
                    continue

                # load token info
                contract_address = log.get('address', '').lower()
                if self._cache_contract_info.get(contract_address) is None:
                    info = await self.get_erc20_or_erc721_info(contract_address=contract_address)
                    self._cache_contract_info[contract_address] = info
                    if ETHDataTypes.TOKEN.value in self.data_types:
                        yield ERCTokenItem(**info)
                contract_info = self._cache_contract_info[contract_address]

                # extract erc20 transfer
                if not contract_info['is_erc721']:
                    continue
                yield ERC721TokenTransferItem(
                    transaction_hash=log.get('transactionHash', ''),
                    log_index=self.hex_to_dec(log.get('logIndex')),
                    block_number=self.hex_to_dec(log.get('blockNumber')),
                    timestamp=kwargs['timestamp'],
                    address_from=self.word_to_address(topics_with_data[1]),
                    address_to=self.word_to_address(topics_with_data[2]),
                    token_id=self.hex_to_dec(topics_with_data[3]),
                    contract_address=contract_address,
                    token_symbol=contract_info.get('token_symbol'),
                )

        def _generate_block_erc1155_transfers(logs: list, **kwargs):
            if ETHDataTypes.ERC1155.value not in self.data_types:
                return

            # extract erc1155 token transfers
            for log in logs:
                topics = log.get('topics')
                if topics[0] != self.ERC1155_SINGLE_TRANSFER_TOPIC and \
                        topics[0] != self.ERC1155_BATCH_TRANSFER_TOPIC:
                    continue

                # load logs data
                data = log.get('data')
                words = self.split_to_words(data)
                words = [self.hex_to_dec(word) for word in words]
                contract_address = log.get('address', '').lower()

                # generate token info
                if self._cache_contract_info.get(contract_address) is None:
                    info = {
                        'address': contract_address,
                        'is_erc20': False,
                        'is_erc721': False,
                        'is_erc1155': True,
                        'token_symbol': '',
                        'decimals': -1,
                        'total_supply': -1
                    }
                    self._cache_contract_info[contract_address] = info
                    if ETHDataTypes.TOKEN.value in self.data_types:
                        yield ERCTokenItem(**info)

                # generate erc1155 token single transfers
                if topics[0] == self.ERC1155_SINGLE_TRANSFER_TOPIC:
                    if len(words) != 2:
                        continue
                    yield ERC1155TokenTransferItem(
                        transaction_hash=log.get('transactionHash', ''),
                        log_index=self.hex_to_dec(log.get('logIndex')),
                        block_number=self.hex_to_dec(log.get('blockNumber')),
                        timestamp=kwargs['timestamp'],
                        address_operator=self.word_to_address(topics[1]),
                        address_from=self.word_to_address(topics[2]),
                        address_to=self.word_to_address(topics[3]),
                        token_ids=[words[0]],
                        values=[words[1]],
                        contract_address=contract_address,
                    )

                # generate erc1155 token batch transfers
                if topics[0] == self.ERC1155_BATCH_TRANSFER_TOPIC:
                    if len(words) < 4 or len(words) % 2 != 0:
                        continue
                    words = words[3:]
                    mid_idx = len(words) >> 1
                    yield ERC1155TokenTransferItem(
                        transaction_hash=log.get('transactionHash', ''),
                        log_index=self.hex_to_dec(log.get('logIndex')),
                        block_number=self.hex_to_dec(log.get('blockNumber')),
                        timestamp=kwargs['timestamp'],
                        address_operator=self.word_to_address(topics[1]),
                        address_from=self.word_to_address(topics[2]),
                        address_to=self.word_to_address(topics[3]),
                        token_ids=words[:mid_idx],
                        values=words[mid_idx + 1:],
                        contract_address=contract_address,
                    )

        try:
            # load data from response
            func_name = 'parse_eth_get_logs'
            self.log(message='On {}, with {}'.format(func_name, str(kwargs)), level=logging.DEBUG)
            result = json.loads(response.text)
            result = result.get('result')
            if result is None:
                yield self.handle_response_error(
                    response=response,
                    func_name=func_name,
                    cb_kwargs=kwargs
                )
                return

            # extract logs of block
            for item in _generate_block_logs(result, **kwargs):
                yield item

            # extract erc20 token transfer of block
            async for item in _generate_block_erc20_transfers(result, **kwargs):
                yield item

            # extract erc721 token transfer of block
            async for item in _generate_block_erc721_transfers(result, **kwargs):
                yield item

            # extract erc1155 token transfer of block
            for item in _generate_block_erc1155_transfers(result, **kwargs):
                yield item

        finally:
            # generate block number for recording crawl progress
            latest_blk = self._block_recorder.record(kwargs['block_number'])
            if latest_blk is not None:
                yield BlockNumberItem(block_number=latest_blk)

    def gen_requests(self, start_blk: int, end_blk: int):
        for blk in range(start_blk, end_blk + 1):
            yield self.get_request_eth_get_block_by_number(
                block_number=blk,
                priority=end_blk - blk,
                cb_kwargs={
                    'block_number': blk,
                    'priority': end_blk - blk
                }
            )

    def handle_response_error(self, response: scrapy.http.Response, func_name: str, cb_kwargs: dict):
        retry_key = 'retry_{}'.format(func_name)
        if cb_kwargs.get(retry_key, 0) >= self.max_retry:
            self.log(
                message="Result field is None on '{}' after #{} retry with {}, ".format(
                    func_name, self.max_retry, cb_kwargs
                ) + "please ensure that whether the provider is available.",
                level=logging.ERROR
            )
            return
        cb_kwargs[retry_key] = cb_kwargs.get(retry_key, 0) + 1
        return response.request.replace(cb_kwargs=cb_kwargs)

    def handle_request_error(self, failure):
        if not any([
            failure.check(HttpError),
            failure.check(TimeoutError, TCPTimedOutError),
            failure.check(DNSLookupError)
        ]):
            return

        # reload context data and log out
        request = failure.request
        kwargs = request.cb_kwargs
        block_number = kwargs['block_number']
        self.log(
            message='Get error when fetching {} with {}, callback args {}'.format(
                request.url, request.body, str(kwargs)
            ),
            level=logging.WARNING
        )

        # generate block number for recording crawl progress
        # if the request failed
        latest_blk = self._block_recorder.record(block_number)
        if latest_blk is not None:
            yield BlockNumberItem(block_number=latest_blk)

    def get_request_eth_block_number(self) -> scrapy.Request:
        return scrapy.Request(
            url=self.provider_bucket.get(),
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "method": "eth_blockNumber",
                "params": [],
                "id": 1,
                "jsonrpc": "2.0"
            }),
            callback=self.parse_eth_block_number,
        )

    def get_request_eth_get_block_by_number(self, block_number: int, priority: int, cb_kwargs: dict) -> scrapy.Request:
        return scrapy.Request(
            url=self.provider_bucket.get(),
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [
                    hex(block_number),
                    True
                ],
                "id": 1
            }),
            callback=self.parse_eth_get_block_by_number,
            errback=self.handle_request_error,
            priority=priority,
            cb_kwargs=cb_kwargs,
        )

    def get_request_trace_block(self, block_number: int, priority: int, cb_kwargs: dict) -> scrapy.Request:
        return scrapy.Request(
            url=self.provider_bucket.get(),
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "method": "trace_block",
                "params": [
                    hex(block_number)
                ],
                "id": 1,
                "jsonrpc": "2.0"
            }),
            callback=self.parse_trace_block,
            errback=self.handle_request_error,
            priority=priority,
            cb_kwargs=cb_kwargs
        )

    def get_request_eth_get_logs(self, block_number: int, priority: int, cb_kwargs: dict) -> scrapy.Request:
        topics = list()
        if ETHDataTypes.ERC20.value in self.data_types or \
                ETHDataTypes.ERC721.value in self.data_types:
            topics.append(self.ERC20_TRANSFER_TOPIC)
        if ETHDataTypes.ERC1155.value in self.data_types:
            topics.append(self.ERC1155_SINGLE_TRANSFER_TOPIC)
            topics.append(self.ERC1155_BATCH_TRANSFER_TOPIC)
        if ETHDataTypes.LOG.value in self.data_types:
            topics = list()

        return scrapy.Request(
            url=self.provider_bucket.get(),
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "method": "eth_getLogs",
                "params": [
                    {
                        "fromBlock": hex(block_number),
                        "toBlock": hex(block_number),
                        "topics": [topics, ]
                    }
                ],
                "id": 1,
                "jsonrpc": "2.0"
            }),
            callback=self.parse_eth_get_logs,
            errback=self.handle_request_error,
            priority=priority,
            cb_kwargs=cb_kwargs
        )

    async def _gather_eth_call_result(self, tx_objs: [dict], output_types: [[str]]) -> [Union[tuple, None]]:
        async def _eth_call(_tx_obj: dict, _output_types: [str]) -> Union[tuple, None]:
            await self.aiohttp_lock.acquire()

            result = None
            req_data = {
                "method": "eth_call",
                "params": [
                    _tx_obj,
                    'latest'
                ],
                "id": 1,
                "jsonrpc": "2.0"
            }
            url = self.provider_bucket.get()
            client = aiohttp.ClientSession()
            try:
                response = await client.post(
                    url=url,
                    json=req_data
                )
                data = await response.read()
                data = json.loads(data)
                data = data.get('result')
                await client.close()

                if not isinstance(data, str) or data == '0x':
                    return None
                data = bytes.fromhex(data[2:])
                result = self._web3.codec.decode_abi(_output_types, data)
            except:
                self.log(
                    message="Failed on '_eth_call' but you can ignore it, callback args {}.".format(str(req_data)),
                    level=logging.WARNING
                )
            finally:
                await client.close()
                self.aiohttp_lock.release()
                return result

        assert len(tx_objs) == len(output_types)
        calls = [_eth_call(tx_objs[i], output_types[i]) for i in range(len(tx_objs))]
        calls = [asyncio.create_task(call) for call in calls]
        return await asyncio.gather(*calls)

    async def get_erc20_or_erc721_info(self, contract_address: str) -> dict:
        # fetch token symbol
        symbol = None
        for result in await self._gather_eth_call_result(
                tx_objs=[
                    {'to': contract_address, "data": self._web3.keccak(text='symbol()').hex()[:2 + 8]},
                    {'to': contract_address, "data": self._web3.keccak(text='SYMBOL()').hex()[:2 + 8]},
                    {'to': contract_address, "data": self._web3.keccak(text='symbol()').hex()[:2 + 8]},
                    {'to': contract_address, "data": self._web3.keccak(text='SYMBOL()').hex()[:2 + 8]},
                ],
                output_types=[
                    ["string", ],
                    ["string", ],
                    ["bytes32", ],
                    ["bytes32", ],
                ]
        ):
            if result is None:
                continue
            result = result[0]
            symbol = self._bytes_to_string(result) if isinstance(result, bytes) else result
            break

        # fetch decimals if erc20
        decimals = None
        for result in await self._gather_eth_call_result(
                tx_objs=[
                    {'to': contract_address, "data": self._web3.keccak(text='decimals()').hex()[:2 + 8]},
                    {'to': contract_address, "data": self._web3.keccak(text='DECIMALS()').hex()[:2 + 8]},
                ],
                output_types=[
                    ["uint8", ],
                    ["uint8", ],
                ]
        ):
            if result is None:
                continue
            decimals = result[0]
            break

        # fetch total supply if erc20
        total_supply = None
        if decimals is not None:
            result = await self._gather_eth_call_result(
                tx_objs=[{'to': contract_address, "data": self._web3.keccak(text='totalSupply()').hex()[:2 + 8]}],
                output_types=[["uint256"]]
            )
            total_supply = result[0][0] if result[0] is not None else None

        return {
            'address': contract_address,
            'is_erc20': True if decimals is not None else False,
            'is_erc721': True if decimals is None else False,
            'is_erc1155': False,
            'token_symbol': symbol if symbol is not None else '',
            'decimals': decimals if decimals is not None else -1,
            'total_supply': total_supply if total_supply is not None else -1,
        }

    async def get_created_contract_address(self, txhash: str) -> dict:
        await self.aiohttp_lock.acquire()
        result = None
        req_data = {
            "method": "eth_getTransactionReceipt",
            "params": [
                txhash,
            ],
            "id": 1,
            "jsonrpc": "2.0"
        }
        url = self.provider_bucket.get()
        client = aiohttp.ClientSession()
        try:
            response = await client.post(
                url=url,
                json=req_data
            )
            data = await response.read()
            data = json.loads(data)
            data = data.get('result')
            await client.close()

            result = data.get('contractAddress')
        except:
            self.log(
                message="Failed on 'eth_getTransactionReceipt' but you can ignore it, callback args {}.".format(
                    str(req_data)),
                level=logging.WARNING
            )
        finally:
            await client.close()
            self.aiohttp_lock.release()
            return result

    @staticmethod
    def chunk_string(string, length):
        return (string[0 + i:length + i] for i in range(0, len(string), length))

    @staticmethod
    def split_to_words(data: str) -> list:
        if data and len(data) > 2:
            data_without_0x = data[2:]
            words = list(BlocksETHSpider.chunk_string(data_without_0x, 64))
            words_with_0x = list(map(lambda word: '0x' + word, words))
            return words_with_0x
        return []

    @staticmethod
    def word_to_address(param: str) -> str:
        if param is None:
            return ''
        if len(param) >= 40:
            return ('0x' + param[-40:]).lower()
        else:
            return param.lower()

    @staticmethod
    def hex_to_dec(hex_string: str) -> int:
        if hex_string is None:
            return -1
        try:
            return int(hex_string, 16)
        except ValueError:
            return -1

    @staticmethod
    def _bytes_to_string(data: bytes):
        if data is None:
            return ''
        try:
            b = data.decode('utf-8')
        except UnicodeDecodeError as _:
            return ''
        return b


class BlockRecorder:
    def __init__(self, start_blk: int):
        self._block_records = list()
        self._latest_block = start_blk - 1
        self._signal = dict()

    def wait(self, block_number: int):
        self._signal[block_number] = self._signal.get(block_number, 0) - 1

    def record(self, block_number: int) -> Union[int, None]:
        # insert block number list if signal gotten
        self._signal[block_number] += 1
        if self._signal[block_number] < 0:
            return
        del self._signal[block_number]

        # find insert index
        length = len(self._block_records)
        insert_idx = 0
        while insert_idx < length and \
                self._block_records[insert_idx] < block_number:
            insert_idx += 1
        self._block_records.insert(insert_idx, block_number)

        # find continuous index sequence
        split_idx = 0
        while split_idx < length + 1 and \
                self._latest_block + 1 == self._block_records[split_idx]:
            self._latest_block += 1
            split_idx += 1

        # return last block number
        if split_idx == 0:
            return None
        self._block_records = self._block_records[split_idx:]
        return self._latest_block
