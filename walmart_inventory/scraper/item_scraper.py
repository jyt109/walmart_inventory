__author__ = 'jeffreytang'

from threading import Thread
from multiprocessing import Pool
import os
from time import sleep

import requests
from bs4 import BeautifulSoup
from pymongo.errors import DuplicateKeyError

from walmart_inventory.scraper.base_scraper import BaseScraper
from walmart_inventory.utils.files import get_full_path


class ItemScraperVar(BaseScraper):

    def __init__(self):
        super(self.__class__, self).__init__()
        self.item_link_template = self.base_url + '/ip/%s?reviews_limit=100&'
        self.page_link_fname = get_full_path('data/page_links.txt')
        self.log_file = open(get_full_path('data/log.txt'), 'w')


item_scraper = ItemScraperVar()

item_link_template = item_scraper.item_link_template
n_cores = item_scraper.n_cores
mongo = item_scraper.mongo
page_link_fname = item_scraper.page_link_fname
log_file = item_scraper.log_file


def scrape_item_content(category_item_link_tup):
    category, item_link = category_item_link_tup
    try:
        response = requests.get(item_link)
        try:
            if response.status_code == 200:
                mongo.html_tab.insert({'_id': item_link, 'category': category,
                                       'html': unicode(response.content, errors='replace')})
            else:
                raise Exception('Status code %s' % response.status_code)
        except DuplicateKeyError:
            print 'Item already exists!'
        sleep(0.8)
    except Exception as e:
        print e
        log_file.write(item_link + '\n')

def scrape_item_content_threading(page_link):
    category = page_link.split('/')[-2]
    print category

    response = requests.get(page_link)
    if response.status_code != 200:
        print 'Status code %d' % response.status_code

    soup = BeautifulSoup(response.content)
    item_link_lst = [item_link_template % tag['data-item-id']
                     for tag in soup.select('.js-tile.tile-grid-unit')]

    category_item_link_tup_lst = zip([category] * len(item_link_lst), item_link_lst)

    jobs = []
    for i, category_item_link_tup in enumerate(category_item_link_tup_lst):
        thread = Thread(target=scrape_item_content, args=(category_item_link_tup,))
        jobs.append(thread)
        print 'started %s-%d' % (category, i)
        thread.start()
        sleep(0.8)

    for j in jobs:
        j.join()

def scrape_page_parallel():
    if os.path.isfile(page_link_fname):
        pool = Pool(processes=n_cores)
        page_links = map(lambda x: x.strip(), open(page_link_fname).readlines())
        pool.map(scrape_item_content_threading, page_links)
    else:
        raise Exception('Page Link File does not exist!')

    return 1
