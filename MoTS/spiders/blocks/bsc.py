from MoTS.spiders.blocks.eth import BlocksETHSpider


class BlocksBSCSpider(BlocksETHSpider):
    name = 'blocks.bsc'
    net = 'bsc'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # api url
        self.base_api_url = 'https://api.bscscan.com/api'
