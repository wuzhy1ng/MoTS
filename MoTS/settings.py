# Scrapy settings for MoTS project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from pathlib import Path

from scrapy.utils.reactor import install_reactor

# Project direction configure
BASE_DIR = Path(__file__).resolve().parent.parent

BOT_NAME = 'MoTS'

SPIDER_MODULES = ['MoTS.spiders']
NEWSPIDER_MODULE = 'MoTS.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'MoTS (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 1.0
# RANDOMIZE_DOWNLOAD_DELAY = True
DOWNLOAD_TIMEOUT = 180

# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 32
# CONCURRENT_REQUESTS_PER_IP = 32

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
    'Accept-Encoding': 'gzip',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'MoTS.middlewares.BlockchainspiderSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#     'MoTS.middlewares.RequestCacheMiddleware': 901,
# }

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'MoTS.pipelines.LabelsPipeline': 300,
    'MoTS.pipelines.BlockPipeline': 303,
    'MoTS.pipelines.BlockNumberPipeline': 304,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = r''
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
# HTTPCACHE_GZIP = True

# Reactor configure
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')

# Log configure
LOG_LEVEL = 'INFO'

# The response size (in bytes) that downloader will start to warn.
DOWNLOAD_WARNSIZE = 33554432 * 2

# Blockscan APIKey configure
APIKEYS_BUCKET = 'MoTS.utils.bucket.StaticAPIKeyBucket'
APIKEYS = {
    "btc": [
        "d613395403d64258a633a2d3764ce21e",
    ],
    "eth": [
        "S1QQC4GQXSDPBX54K73MX77KIFERJI8IFU",
    ],
    "bsc": [
        "FNK9YSU72DAF8HMVGXBFGWFKFAA4QBQ8Q5",
    ],
    "polygon": [
        "2JQPMV7D12PWTYB59I9698Z4XQ3ZK68JA8",
    ],
    "heco": [
        "7SMM4F12EQRRGKYCN2VK6I48R7M8CFNE8R"
    ]
}

# Providers configure
PROVIDERS_BUCKET = 'MoTS.utils.bucket.StaticProvidersBucket'
PROVIDERS = {
    'eth': [
        "https://eth-mainnet.alchemyapi.io/v2/VvbmI0CclfYE21JasEph4_BwcyeVnctj",
        "https://eth-mainnet.alchemyapi.io/v2/dWIYQLA2KfoIcNimItxMtpl8ZUBo8yoc",
        "https://eth-mainnet.alchemyapi.io/v2/1_l0cbEYqpKWWMzCk56nDki3vwzX5KzC",
    ]
}
