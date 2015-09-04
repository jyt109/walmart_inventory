__author__ = 'jeffreytang'

from walmart_inventory.scraper import item_scraper

if __name__ == '__main__':
    item_scraper.scrape_page_parallel()
