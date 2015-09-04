__author__ = 'jeffreytang'

import multiprocessing

from mongo.base_mongo import BaseMongo


class BaseScraper(object):

    def __init__(self):
        self.base_url = 'http://www.walmart.com'
        self.n_cores = multiprocessing.cpu_count()
        self.mongo = BaseMongo()

