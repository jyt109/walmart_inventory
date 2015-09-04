__author__ = 'jeffreytang'

import os

from bs4 import BeautifulSoup
from dateutil import parser
import requests
from pymongo.errors import DuplicateKeyError
from walmart_inventory.mongo.base_mongo import BaseMongo
import multiprocessing
from walmart_inventory.utils.files import get_full_path
from multiprocessing import Pool


class HTMLParser(BaseMongo):
    def __init__(self, image_path='data/images'):
        super(self.__class__, self).__init__()
        self.image_dir = get_full_path(image_path)

html_parser = HTMLParser()
image_dir = html_parser.image_dir
item_tab = html_parser.item_tab
html_tab = html_parser.html_tab


def parse_review(soup):
    review_tag_lst = soup.select('.customer-review')
    review_lst = []
    if len(review_tag_lst) > 0:
        for review in review_tag_lst:
            review_dict = dict()
            helpful_num = None
            not_helpful_num = None

            review_dict['review_title'] = review.select('.customer-review-body .customer-review-title')[0].text
            review_dict['review_date'] = parser.parse(review.select('.customer-review-body .customer-review-date')[0]
                                                      .text)
            review_dict['review_text'] = review.select('.customer-review-body .js-customer-review-text')[0].text
            review_dict['stars'] = float(review.select('.customer-review-body .stars .visuallyhidden')[0].text
                                         .strip('stars').strip()) / 5
            # The number of people find the review helpful
            helpful_tag = review.select('.customer-review-body .js-vote-positive-count')
            if len(helpful_tag) > 0:
                helpful_num = int(helpful_tag[0].text)
            review_dict['helpful'] = helpful_num
            # The number of people find the review unhelpful
            not_helpful_tag = review.select('.customer-review-body .js-vote-negative-count')
            if len(not_helpful_tag) > 0:
                not_helpful_num = int(not_helpful_tag[0].text)
            review_dict['not_helpful'] = not_helpful_num
            # Recommend to a friend
            review_dict['recommend'] = review.select('.customer-info .customer-attributes span')[0].text == 'Yes'
            review_lst.append(review_dict)
    return review_lst

def save_image(image_url):
    f = open(os.path.join(image_dir, image_url.split('/')[-1]), 'w')
    f.write(requests.get(image_url).content)
    f.close()

def parse_item(html):
    soup = BeautifulSoup(html)

    # item can be out of stock, then there is no price
    price = None
    avg_stars = None
    about = None

    # Get Name. If there is no name then just ignore
    try:
        name = soup.select('[itemprop="name"]')[0].text.strip()
    except IndexError:
        raise Exception('There is no name for this item')
    # Price
    price_str = str(soup.select('[itemprop="price"]')[0].text.strip().strip('$').strip())
    if price_str:
        price = float(price_str)
    # Get Image
    image_url = soup.select('[itemprop="image"]')[0]['src'].strip()
    save_image(image_url)
    # Get Average stars
    avg_stars_tags = soup.select('[itemprop="ratingValue"] .visuallyhidden')
    if len(avg_stars_tags) > 0:
        avg_stars = avg_stars_tags[0].text.strip('stars').strip()
    # Get the About paragraph
    about_tags = soup.select('.product-about .module p')
    if len(about_tags) > 0:
        about = about_tags[0].text
    # Get Reviews
    review_lst = parse_review(soup)

    return dict(price=price, image_url=image_url, name=name,
                avg_stars=avg_stars, about=about,
                review_lst=review_lst)

def insert_into_mongo(d):
    try:
        item_tab.insert(d)
    except DuplicateKeyError:
        print 'Item already exists!'

def parse_html(html_item):
    try:
        print html_item['_id']
        parsed_dict = parse_item(html_item['html'])
        parsed_dict['link'] = html_item['_id']
        parsed_dict['_id'] = int(html_item['_id'].split('/')[-1].split('?')[0])
        parsed_dict['category'] = html_item['category']
        insert_into_mongo(parsed_dict)
    except Exception as e:
        print html_item['_id'] + '---' + str(e)

def run():
        item_entry_lst = list(html_tab.find({}))
        counter = 0
        for html_item in item_entry_lst:
            counter += 1
            if counter % 1000 == 0:
                print 'Itemized %s' % counter
            parse_html(html_item)




