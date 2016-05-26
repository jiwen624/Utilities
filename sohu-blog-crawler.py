#!/usr/bin/env python3

import requests
import re
import sys
import pathlib
import os
import time
from bs4 import BeautifulSoup
from multiprocessing import Pool


def fetch_ebi(url):
    """
    Access the url provided and fetch the ebi string from the HTML page
    :param url:
    :return: ebi
    """
    try:
        rsp = requests.get(url)
    except requests.exceptions.RequestException as e:
        print('Network failure({}): {}'.format(url, e))
        raise

    ebi_pattern = r"_ebi = '(.*)'"
    if rsp.status_code == 200:
        ebi = re.search(ebi_pattern, rsp.text).group(1)
    else:
        print('HTTP Error: {}'.format(rsp.status_code))
        ebi = None

    return ebi


def blog_items_url(url, ebi, pageno):
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
            print('Network failure({}): {}'.format(url, e))
            raise

        if rsp.status_code == 200:
            if 'data-entryid' not in rsp.text:
                break

            # print('>>Page {}: '.format(page_no))
            for entry_date, entry_url, entry_title in blog_entry(rsp.text):
                yield entry_date, entry_url, entry_title
        else:
            print('{} {}'.format(rsp.status_code, page_url))
            break

    # print('Reaching the end at page {}'.format(page_no))


def blog_entry(html):
    """
    This is a generator which parses the html and yield a blog entry each time.
    :param html:
    :return:
    """
    blog_entry_pattern = r'<span class="date">(.*)</span>\s*<a href="(.*)"  target="_blank" class="list-title">(.*)</a>'
    for m_obj in re.finditer(blog_entry_pattern, html):
        yield m_obj.group(1), m_obj.group(2), m_obj.group(3)


def create_html_file(title, url, date, body):
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



def get_blog_content(url, title, date):  # TODO
    """
    This function fetch the url and parse it to get the blog content.
    :param url:
    :return:
    """
    try:
        rsp = requests.get(url)
    except requests.exceptions.RequestException as e:
        print('Network failure({}): {}'.format(url, e))
        raise

    soup = BeautifulSoup(rsp.text, 'lxml')

    images = soup.find("div", {"id": "main-content"}).find_all('img')
    for img in images:
        img_url = img.get('src')
        img_file_name = img_url.split('/')[-1]

        try:
            rsp = requests.get(img_url)
        except requests.exceptions.RequestException as e:
            print('Network failure({}): {}'.format(img_url, e))
            raise

        if rsp.status_code == 200:
            with open('resources/'+img_file_name, 'wb') as f:
                f.write(rsp.content)
        else:
            print('Failed to download image with url {}'.format(img_url))

    main_content = re.match(r'^(.*?)<div class="clear">',
                            str(soup.find("div", {"id": "main-content"})),
                            re.DOTALL
                            ).group(1) + '</div>'

    html = create_html_file(title, url, date, main_content)
    relative_html = re.sub(r'http:(?:.*/)*(.*jpg)', r'resources/\1', html)

    with open('{}_{}.html'.format(date, title), 'w') as blog_page_html:
        blog_page_html.write(relative_html)

    print('({}) Fetched: {}  [{}]  {}'.format(os.getpid(), date, url, title))



def main():
    """
    The main function
    :return:
    """
    start = time.time()

    url = 'http://zhaozilongkun.blog.sohu.com'
    worker_num = 8  # process num is hardcoded to 4. Change it if needed.
    # Hardcoded for now. It should be parameter passed to get_blog_content()
    d = pathlib.Path('resources')
    try:
        d.mkdir(parents=True, exist_ok=True)
    except OSError:
        raise

    print('Start fetching {}...'.format(url))
    p = Pool(worker_num)
    blog_num = 0

    for entry_date, entry_url, entry_title in blog_items(url):
        # print('Outer: {}  [{}]    {}'.format(entry_date, entry_url, entry_title))
        p.apply_async(get_blog_content, args=(entry_url, entry_title, entry_date))
        blog_num += 1

    # print('All page index fetched, waiting for page downloading.')
    p.close()
    p.join()

    elapsed = (time.time() - start)
    print("Fetched {} blogs by {} workers in {} seconds. Bye.".format(blog_num, worker_num, elapsed))

    return 0


if __name__ == '__main__':
    sys.exit(main())
    # get_blog_content('http://zhaozilongkun.blog.sohu.com/40751815.html', '长大了', '2012-02-04')

# TODO: 1. parse the html page of each blog entry and download the pages/videos.
# TODO: 2. create a chm file for the html files
# TODO: 3. multi-processing

