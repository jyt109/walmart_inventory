__author__ = 'jeffreytang'

from pymongo import MongoClient

class BaseMongo(object):

    def __init__(self):
        self.db_name = 'walmart_inventory'
        self.html_tab_name = 'html'
        self.item_tab_name = 'item'

        c = MongoClient()
        self.db = c[self.db_name]
        self.html_tab = self.db[self.html_tab_name]
        self.item_tab = self.db[self.item_tab_name]
