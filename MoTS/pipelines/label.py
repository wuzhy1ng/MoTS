import json
import os

from MoTS.items import LabelItem


class LabelsPipeline:
    def __init__(self):
        self.file = None

    def process_item(self, item, spider):
        if not isinstance(item, LabelItem):
            return item

        # init file from filename
        if self.file is None:
            fn = os.path.join(spider.out_dir, spider.name)
            if not os.path.exists(spider.out_dir):
                os.makedirs(spider.out_dir)
            self.file = open(fn, 'w')

        # write item
        json.dump({**item}, self.file)
        self.file.write('\n')
        return item

    def close_spider(self, spider):
        if self.file is not None:
            self.file.close()
