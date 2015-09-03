__author__ = 'jeffreytang'

from base_link_getter import BaseLinkGetter
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup


# Function to make multiprocessing work in a class context.
def unwrap_get_items_from_category_link(category_link_lst):
    return PageLinkGetter.get_items_from_category_link(category_link_lst)

class PageLinkGetter(BaseLinkGetter):
    def __init__(self, taxonomy_id='976759', out_file_name='page_links.txt'):
        super(self.__class__, self).__init__()
        self.taxonomy_id = taxonomy_id
        self.out_file_name = out_file_name

    def get_category_links(self):
        html = requests.get('http://www.walmart.com/cp/%s' % self.taxonomy_id).content
        soup = BeautifulSoup(html)
        lst = soup.select('a[href*=food]')
        links = [tag['href'] for tag in lst if tag['href'].startswith('/browse/food') and ',' not in tag['href']]
        return [self.base_url + link for link in links]

    def get_items_from_category_link_parallel(self, category_link_lst):
        pool = Pool(processes=self.n_cores)
        return pool.map(unwrap_get_items_from_category_link, category_link_lst)

    @staticmethod
    def get_items_from_category_link(category_link):
        print category_link
        category_first_page_soup = BeautifulSoup(requests.get(category_link).content)
        # Get the last page number
        page_lst_css_selector = '.paginator.no-top-border ul.paginator-list li'
        last_page_number = int(category_first_page_soup.select(page_lst_css_selector)[-1].text)
        page_template = 'page=%d'
        # Links for all pages for the category
        return ['%s?%s' % (category_link, page_template % i) for i in range(1, last_page_number)]

    @staticmethod
    def write_to_csv(link_lst_of_lst, f_name):
        f = open(f_name, 'w')
        for link_lst in link_lst_of_lst:
            for link in link_lst:
                f.write(link + '\n')
        f.close()

    def run(self):
        category_links = self.get_category_links()
        link_lst_of_lst = self.get_items_from_category_link_parallel(category_links)
        self.write_to_csv(link_lst_of_lst, self.out_file_name)

if __name__ == '__main__':
    plg = PageLinkGetter()
    plg.run()
