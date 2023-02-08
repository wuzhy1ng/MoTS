from MoTS.spiders.blocks.eth import BlocksETHSpider


class BlocksPolygonSpider(BlocksETHSpider):
    name = 'blocks.polygon'
    net = 'polygon'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # api url
        self.base_api_url = 'https://api.polygonscan.com/api'
