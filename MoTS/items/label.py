import scrapy


class LabelItem(scrapy.Item):
    _id = scrapy.Field()  # str
    net = scrapy.Field()  # str
    category = scrapy.Field()  # str
    labels = scrapy.Field()  # [str]
    info = scrapy.Field()  # dict
