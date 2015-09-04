__author__ = 'jeffreytang'

import subprocess
import sys

if __name__ == '__main__':
    command_name = sys.argv[1]
    if command_name == 'scrape_page_link':
        subprocess.call('python -m walmart_inventory.scripts.category_link_scraper', shell=True)
    if command_name == 'scrape_items':
        subprocess.call('python -m walmart_inventory.scripts.item_scraper', shell=True)
    if command_name == 'parse_item_html':
        subprocess.call('python -m walmart_inventory.scripts.item_html_parser', shell=True)
    else:
        raise NotImplementedError('Please enter a correct command!')

