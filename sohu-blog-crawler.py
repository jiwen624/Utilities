#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import re
import sys
import pathlib
import os
import time
import argparse
import logging
from bs4 import BeautifulSoup
from multiprocessing import Pool

log = logging.getLogger(__name__)

def fetch_ebi(url):
    """
    Access the url provided and fetch the ebi string from the HTML page
    :param url:
    :return: ebi
    """
    try:
        rsp = requests.get(url)
    except requests.exceptions.RequestException as e:
        log.error('Failed to get ebi: network failure({}): {}'.format(url, e))
        raise

    ebi_pattern = r"_ebi = '(.*)'"
    if rsp.status_code == 200:
        ebi = re.search(ebi_pattern, rsp.text).group(1)
    else:
        log.error('Failed to get ebi: HTTP error: {}'.format(rsp.status_code))
        ebi = None

    return ebi


def blog_items_url(url, ebi, pageno):
    """
    This function is used to build the url to fetch blog page list.
    :param url:
    :param ebi:
    :param pageno:
    :return:
    """
    url = url.rstrip('/')
    return "{}/action/v_frag-ebi_{}-pg_{}/entry/".format(url, ebi, pageno)


def blog_items(url):
    """
    This is a generator which returns a blog item when invoked.
    :param url:
    :param ebi:
    :return:
    """
    ebi = fetch_ebi(url)

    for page_no in range(1, 1000):
        page_url = blog_items_url(url, ebi, page_no)
        try:
            rsp = requests.get(page_url)
        except requests.exceptions.RequestException as e:
            log.error('(Master) Failed to get page list: ({}): {}'.format(url, e))
            raise

        if rsp.status_code == 200:
            if 'data-entryid' not in rsp.text:
                break

            log.debug('>> Page {} << '.format(page_no))
            for entry_date, entry_url, entry_title in blog_entry(rsp.text):
                log.debug('(Master) Got blog entries {} {} {}'.format(entry_date, entry_url, entry_title))
                yield entry_date, entry_url, entry_title
        else:
            log.error('Failed to get page list: HTTP error {} {}'.format(rsp.status_code, page_url))
            break

    log.debug('(Master) Reaching the end of the page list.')


def blog_entry(html):
    """
    This is a generator which parses the html and yield a blog entry each time.
    :param html:
    :return:
    """
    blog_entry_pattern = r'<span class="date">(.*)</span>\s*<a href="(.*)"  target="_blank" class="list-title">(.*)</a>'
    for m_obj in re.finditer(blog_entry_pattern, html):
        log.debug('(Master) Producing blog entries {} {} {}'.format(m_obj.group(1), m_obj.group(2), m_obj.group(3)))
        yield m_obj.group(1), m_obj.group(2), m_obj.group(3)


def create_html_file(title, url, date, body):
    """
    Create a simple html file...
    :param title:
    :param url:
    :param date:
    :param body:
    :return:
    """
    meta = """
    <!DOCTYPE html>
        <html>
            <title></title>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            <body>
                <h1>{}</h1>
                <a href="{}">{}</a>
                <p>{}</p>
                <p>{}</p>
            </body>
        </html>
    """
    return meta.format(title, url, url, date, body)


def cut_url(matchobj):
    log.debug('Before: {} After: {}'.format(matchobj.group(0), matchobj.group(0).split('/')[-1]))
    return matchobj.group(0).split('/')[-1]


def get_blog_content(url, title, date, base_dir='.'):
    """
    This is the main function to fetch the url and parse it to get the blog content.
    - blog main content
    - blog images
    - blog comments

    :param url:
    :param title:
    :param date:
    :param base_dir:
    :return:
    """
    pid = os.getpid()

    log.debug('({}) Fetching blog: {} {}.'.format(pid, url, title))

    try:
        rsp = requests.get(url)
    except requests.exceptions.RequestException as e:
        log.error('({}) Failed to get blog content: network failure({}): {}'.format(pid, url, e))
        # raise

    soup = BeautifulSoup(rsp.text, 'lxml')

    images = soup.find("div", {"id": "main-content"}).find_all('img')
    for img in images:
        img_url = img.get('src')
        img_file_name = img_url.split('/')[-1]

        log.debug('({}) Fetching images [{}]'.format(pid, img_url))

        try:
            rsp = requests.get(img_url)
        except requests.exceptions.RequestException as e:
            log.error('({}) Failed to get images: network failure({}): {}'.format(pid, img_url, e))
            # raise

        if rsp.status_code == 200:
            try:
                img_file = base_dir + '/resources/'+img_file_name
                log.debug('({}) writing img [{}] to file {}'.format(pid, img, img_file))

                with open(img_file, 'wb') as f:
                    f.write(rsp.content)
            except:  # TODO: reduce the scope
                log.error('({}) file error: {}'.format(pid, img_file))
        else:
            # Continue to fetch the next image/item.
            log.warning('({}) Failed to download image with url {}'.format(pid, img_url))

    log.debug('({}) Fetching html file: {} {}'.format(pid, url, title))
    main_content = re.match(r'^(.*?)<div class="clear">',
                            str(soup.find("div", {"id": "main-content"})),
                            re.DOTALL
                            ).group(1) + '</div>'
    log.debug('({}) main content captured: {} {}'.format(pid, url, title))

    html = create_html_file(title, url, date, main_content)
    log.debug('({}) content html created: {} {}'.format(pid, url, html))

    relative_html = re.sub(r'http://.*?\.(?:jpg|gif|png)', cut_url, html)

    log.debug('({}) relative_html created: {} {}'.format(pid, url, relative_html))

    html_file = base_dir + '/{}_{}.html'.format(date, title)
    log.debug('({}) [{}] writing to file: {}'.format(pid, url, html_file))
    try:
        with open(html_file, 'w') as blog_page_html:
            blog_page_html.write(relative_html)
    except:  # Minimize the scope
        log.error('({}) file error: {}'.format(pid, html_file))

    log.info('({}) Fetched: {}  [{}]  {}'.format(pid, date, url, title))


# def download_blog_item(url, title, date, base_dir='.'):
#     try:
#         get_blog_content(url, title, date, base_dir)
#     except:
#         log.error('Error in get_blog_content')


def main():
    """
    The main function
    :return:
    """
    parser = argparse.ArgumentParser(description="The Utility to backup your sohu blog :P")
    parser.add_argument("url", help="the url of your sohu blog")

    parser.add_argument("-v", help="detailed print( -v: info, -vv: debug)",
                        action='count', default=0)
    parser.add_argument("-d", help="the directory to store your data", default='.')

    parser.add_argument("-n", help="the number of concurrent workers", type=int, default=1)

    args = parser.parse_args()

    if args.v == 0:
        log_level = logging.ERROR
    elif args.v == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    # Set the log level of this module
    logging.basicConfig(level=log_level)

    # Disable the info logging level of requests module
    logging.getLogger("requests").setLevel(logging.WARNING)

    start = time.time()

    if args.url.startswith('http://'):
        url = args.url
    else:
        url = 'http://' + args.url

    worker_num = args.n

    d = args.d
    try:
        pathlib.Path(args.d + '/resources').mkdir(parents=True, exist_ok=True)
    except OSError:
        raise

    log.info('Start fetching {}...'.format(url))
    p = Pool(worker_num)
    blog_num = 0

    for entry_date, entry_url, entry_title in blog_items(url):
        # p.apply_async(get_blog_content, args=(entry_url, entry_title, entry_date, d))
        log.debug('(Master) Preparing to fetch: {} {}'.format(entry_url, entry_title))
        p.apply_async(get_blog_content, args=(entry_url, entry_title, entry_date, d))
        blog_num += 1

    log.debug('All page index fetched, waiting for page downloading.')
    p.close()
    log.debug('Pool closed')
    p.join()
    log.debug('Pool joined')

    elapsed = int(time.time() - start)
    log.info("Fetched {} blogs by {} workers in {} seconds. Bye.".format(blog_num, worker_num, elapsed))

    return 0


if __name__ == '__main__':
    sys.exit(main())
