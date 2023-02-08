from MoTS.spiders.blocks.eth import BlocksETHSpider


class BlocksPolygonSpider(BlocksETHSpider):
    name = 'blocks.heco'
    net = 'heco'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # api url
        self.base_api_url = 'https://api.hecoinfo.com/api'
