# MoTS
A basic implementation of "Know Your Transactions: Real-time and Generic Transaction Semantic Representation on Blockchain & Web3 Ecosystem".

## Installation

Use `pip install` to install dependency:
```shell
pip install -r requiremets.txt
```

## Quickly start

1. transaction data acquisition

Use `blocks.eth` to collect transactions from RPC interfaces e.g.
```shell
scrapy crawl blocks.eth -a start_blk=10000000 -a end_blk=11000000 -a types=external,internal,erc20,erc721,erc1155
```
- **start_blk**: the start block for collecting transactions
- **end_blk**: the end block for collecting transactions. if this option is not set, the data of the latest block will be continuously collected.
- **types**: the collected transactions types.
  `external` denotes external transactions,
  `internal` denotes internal transactions,
  `erc20` denotes ERC20 token transfers,
  `erc721` denotes ERC721 token transfers,
  and `erc1155` denotes ERC1155 token transfers

3. transaction semantics extraction

Use `blocks.semantic.eth` to collect transactions from RPC interfaces e.g.
```shell
scrapy crawl blocks.semantic.eth -a start_blk=10000000 -a end_blk=11000000 -a types=external,internal,erc20,erc721,erc1155
```
- **start_blk**: the start block for collecting transactions
- **end_blk**: the end block for collecting transactions. if this option is not set, the data of the latest block will be continuously collected.
- **types**: the collected transactions types that would be used to compute semantic vectors.
  `external` denotes external transactions,
  `internal` denotes internal transactions,
  `erc20` denotes ERC20 token transfers,
  `erc721` denotes ERC721 token transfers,
  and `erc1155` denotes ERC1155 token transfers

4. crawl transaction semantics labels

Use `blocks.semantic.eth` to collect transactions semantics labels from Etherscan e.g.
```shell
scrapy crawl labels.action -a start_blk=10000000 -a end_blk=11000000
```
- **start_blk**: the start block for collecting transactions
- **end_blk**: the end block for collecting transactions.

## Settings
1. Currency
   see `CONCURRENT_REQUESTS` in `settings.py`.

2. APIKey and RPC interfaces URL
   see `APIKEYS` and `PROVIDERS` in `settings.py`

## Storage
All collected can be found `./data`.
