__author__ = 'jeffreytang'

import multiprocessing

class BaseLinkGetter(object):

    def __init__(self):
        self.base_url = 'http://www.walmart.com'
        self.n_cores = multiprocessing.cpu_count()

