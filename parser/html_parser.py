__author__ = 'jeffreytang'

from mongo.base_mongo import BaseMongo
from bs4 import BeautifulSoup
from utils.files import get_full_path
from dateutil import parser
import requests
import os
from pymongo.errors import DuplicateKeyError

class HTMLParser(BaseMongo):
    def __init__(self, image_path='data/images'):
        super(self.__class__, self).__init__()
        self.image_dir = get_full_path(image_path)

    def parse_review(self, soup):
        review_tag_lst = soup.select('.customer-review')
        review_lst = []
        for review in review_tag_lst:
            review_dict = dict()
            review_dict['review_title'] = review.select('.customer-review-body .customer-review-title')[0].text
            review_dict['review_date'] = parser.parse(review.select('.customer-review-body .customer-review-date')[0]
                                                      .text)
            review_dict['review_text'] = review.select('.customer-review-body .js-customer-review-text')[0].text
            review_dict['stars'] = float(review.select('.customer-review-body .stars .visuallyhidden')[0].text
                                         .strip('stars').strip()) / 5
            review_dict['helpful'] = int(review.select('.customer-review-body .js-vote-positive-count')[0].text)
            review_dict['not_helpful'] = int(review.select('.customer-review-body .js-vote-negative-count')[0].text)
            review_dict['recommend'] = review.select('.customer-info .customer-attributes span')[0].text == 'Yes'
            review_lst.append(review_dict)
        return review_lst

    def save_image(self, image_url):
        f = open(os.path.join(self.image_dir, image_url.split('/')[-1]), 'w')
        f.write(requests.get(image_url).content)
        f.close()

    def parse_item(self, html):
        soup = BeautifulSoup(html)

        price_str = str(soup.select('[itemprop="price"]')[0].text.strip().strip('$').strip())
        price = None # item can be out of stock, then there is no price
        if price_str:
            price = float(price_str)
        image_url = soup.select('[itemprop="image"]')[0]['src'].strip()
        self.save_image(image_url)
        name = soup.select('[itemprop="name"]')[0].text.strip()
        avg_stars = soup.select('[itemprop="ratingValue"] .visuallyhidden')[0].text\
            .strip('stars').strip()
        about = soup.select('.product-about .module p')[0].text
        review_lst = self.parse_review(soup)

        return dict(price=price, image_url=image_url, name=name,
                    avg_stars=avg_stars, about=about,
                    review_lst=review_lst)

    def insert_into_mongo(self, d):
        try:
            self.item_tab.insert(d)
        except DuplicateKeyError:
            print 'Item already exists!'

    def run(self):
        cursor = self.html_tab.find({})
        counter = 0
        for html_item in cursor:
            counter += 1
            if counter % 1000 == 0:
                print 'Itemized %s' % counter
            parsed_dict = self.parse_item(html_item['html'])
            parsed_dict['link'] = html_item['_id']
            parsed_dict['_id'] = int(html_item['_id'].split('/')[-1].split('?')[0])
            parsed_dict['category'] = html_item['category']
            self.insert_into_mongo(parsed_dict)

if __name__ == '__main__':
    html_parser = HTMLParser()
    html_parser.run()








