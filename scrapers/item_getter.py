__author__ = 'jeffreytang'

from threading import Thread
from multiprocessing import Pool
import multiprocessing
import requests
from bs4 import BeautifulSoup
from mongo.base_mongo import BaseMongo
import os
from pymongo.errors import DuplicateKeyError

base_url = 'http://www.walmart.com'
n_cores = multiprocessing.cpu_count()
item_link_template = base_url + '/ip/%s?reviews_limit=100&'
mongo = BaseMongo()
page_link_fname = 'page_links.txt'
log_file = open('log.txt', 'w')

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

    def scrape_item_content(category_item_link_tup):
        category, item_link = category_item_link_tup
        try:
            html = requests.get(item_link).content
            try:
                mongo.html_tab.insert({'_id': item_link, 'category': category, 'html': html})
            except DuplicateKeyError:
                print 'Item already exists!'
        except:
            print 'Item link not accessible: %s' % item_link
            log_file.write('Item link not accessible: %s' % item_link)

    jobs = []
    print category_item_link_tup_lst
    for i, category_item_link_tup in enumerate(category_item_link_tup_lst):
        thread = Thread(target=scrape_item_content, args=(category_item_link_tup,))
        jobs.append(thread)
        print 'started %s-%d' % (category, i)
        thread.start()

    for j in jobs:
        j.join()

def scrape_page_parallel():
    if os.path.isfile(page_link_fname):
        pool = Pool(processes=n_cores)
        page_links = map(lambda x: x.strip(), open(page_link_fname).readlines()) ###
        pool.map(scrape_item_content_threading, page_links)
    else:
        raise Exception('Page Link File does not exist!')

    return 1


if __name__ == '__main__':
    # scrape_item_content_threading('http://www.walmart.com/browse/food/chips/976759_976787_1001390?page=1')
    scrape_page_parallel()
