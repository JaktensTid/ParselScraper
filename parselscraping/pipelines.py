from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from scrapy.conf import settings
from scrapy import log

total = 0
class MongodbPipeLine(object):
    def __init__(self):
        connection = MongoClient(settings['MONGODB_URI'])
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        try:
            #self.collection.insert_one(item)
            self.collection.update_one({'id' : item['id']},
                                       {'$set' : {'Individual Built As Detail' : item['Individual Built As Detail']}})
        except DuplicateKeyError:
            print('Duplicate key error')
        return item
