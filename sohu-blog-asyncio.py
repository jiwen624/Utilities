#!/usr/bin/env python3
"""
This program is a web crawler that download the Sohu blog entries of a specific user.
I wrote this program to backup my wife's blog. :-)
-- Jun 15, 2016    Changed to use asyncio.
"""
import aiohttp
import asyncio
import re
import sys
import pathlib
import os
import time
import argparse
import logging
from bs4 import BeautifulSoup

# log = logging.getLogger(__name__)
log = logging.getLogger('')
fetched_blog_num = 0
ebi_text = ''
failed_url = []


async def fetch_ebi(url):
    """
    Access the url provided and fetch the ebi string from the HTML page

    Used aiohttp/coroutine:
        with aiohttp.ClientSession() as session:
        async with session.get('https://api.github.com/events') as resp:
            print(resp.status)
            print(await resp.text())

    :param url:
    :return: ebi
    """

    global ebi_text

    # Just return it if ebi has been fetched
    if ebi_text:
        return ebi_text

    try:
        with aiohttp.Timeout(20):
            with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    assert resp.status == 200
                    ebi_text = await resp.text()
    except Exception:
        log.error('Failed to fetch ebi.')
        raise  # TODO: Exception handling.

    ebi_pattern = r"_ebi = '(.*)'"

    log.debug('ebi_in_text: {}'.format(ebi_text))
    match = re.search(ebi_pattern, ebi_text)
    if not match:
        log.error('Cannot find ebi, is the url correct?')
        sys.exit(1)  # TODO: 不应该在这里退出, 应该raise exception?

    ebi_text = match.group(1)
    return ebi_text


def blog_items_url(url, ebi, page_no):
    """
    This function is used to build the url to fetch blog page list.
    :param url:
    :param ebi:
    :param page_no:
    :return:
    """
    url = url.rstrip('/')
    return "{}/action/v_frag-ebi_{}-pg_{}/entry/".format(url, ebi, page_no)


async def blog_items(url, page_no):
    """
    This is a generator which returns a blog item when invoked.
    -- Changed with asyncio
    :param url:
    :param page_no:
    :return:
    """
    ebi = await fetch_ebi(url)
    assert ebi is not None

    page_url = blog_items_url(url, ebi, page_no)

    try:
        with aiohttp.Timeout(20):
            with aiohttp.ClientSession() as session:
                async with session.get(page_url) as resp:
                    assert resp.status == 200
                    page_text = await resp.text()
    except Exception:
        log.error('Failed to get page {} of url: {}'.format(page_no, url))
        raise  # TODO: Exception handling.

    if 'data-entryid' not in page_text:
        return None

    log.debug('>> Page {} << '.format(page_no))
    blog_list = list(blog_entry(page_text))

    log.debug('Fetched page {}.'.format(page_no))
    return blog_list


def blog_entry(html):
    """
    This is a generator which parses the html and yield a blog entry each time.
    :param html:
    :return:
    """
    blog_entry_pattern = r'<span class="date">(.*)</span>\s*<a href="(.*)"  target="_blank" class="list-title">(.*)</a>'
    for m_obj in re.finditer(blog_entry_pattern, html):
        log.debug('(Master) Producing blog entries {} {} {}'
                  .format(m_obj.group(1), m_obj.group(2), m_obj.group(3)))
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


def cut_url(match):
    log.debug('Before: {} After: {}'.format(match.group(0), match.group(0).split('/')[-1]))
    return 'resources/' + match.group(0).split('/')[-1]


async def get_blog_content(url, title, date, base_dir='.'):
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
    global failed_url
    global fetched_blog_num
    # pid = os.getpid()
    pid = 0

    log.debug('({}) Fetching blog: {} {}.'.format(pid, url, title))

    try:
        with aiohttp.Timeout(20):
            with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    assert resp.status == 200
                    blog_text = await resp.text()
    except Exception:
        log.error('Failed to get blog content, url: {} title: {} date: {}'
                  .format(url, title, date))
        failed_url.append((date, url, title))
        # Just raise this exception and skip the images of this page.
        raise  # TODO: Exception handling.

    soup = BeautifulSoup(blog_text, 'lxml')

    images = soup.find("div", {"id": "main-content"}).find_all('img')

    for img in images:
        img_url = img.get('src')
        img_file_name = img_url.split('/')[-1]

        log.debug('({}) Fetching image [{}]'.format(pid, img_url))

        try:
            with aiohttp.Timeout(20):
                with aiohttp.ClientSession() as session:
                    async with session.get(img_url) as resp:
                        assert resp.status == 200
                        img_content = await resp.read()
        except Exception:
            log.warning('({}) Failed to download image with url {}, but I will continue.'
                        .format(pid, img_url))
            # Skip the failed images
            # failed_url.append(('0000-00-00', url, 'Image'))
            continue

        img_file = base_dir + '/resources/' + img_file_name
        log.debug('({}) writing img [{}] to file {}'.format(pid, img_url, img_file))

        with open(img_file, 'wb') as f:
            f.write(img_content)

    log.debug('({}) Fetching html file: {} {}'.format(pid, url, title))
    main_content = re.match(r'^(.*?)<div class="clear">',
                            str(soup.find("div", {"id": "main-content"})),
                            re.DOTALL
                            ).group(1) + '</div>'
    log.debug('({}) main content captured: {} {}'.format(pid, url, title))

    html = create_html_file(title, url, date, main_content)

    relative_html = re.sub(r'http://.*?\.(?:jpg|gif|png)', cut_url, html)

    html_file = base_dir + '/{}_{}.html'.format(date, title)
    log.debug('({}) [{}] writing to file: {}'.format(pid, url, html_file))

    with open(html_file, 'w') as blog_page_html:
        blog_page_html.write(relative_html)

    log.info('({}) Fetched: {}  [{}]  {}'.format(pid, date, url, title))
    fetched_blog_num += 1


async def download_blog_item(sem, url, title, date, base_dir='.'):
    async with sem:
        try:
            await get_blog_content(url, title, date, base_dir)
        except Exception as e:
            # Don't raise the exception here, otherwise other coroutines will be stopped.
            log.error('{} Exception catched! {}'.format(os.getpid(), e))


def main():
    """
    The main function
    :return:
    """
    global failed_url
    global fetched_blog_num

    parser = argparse.ArgumentParser(description="The Utility to backup your sohu blog :P")
    parser.add_argument("url", help="the url of your sohu blog")
    parser.add_argument("-v", help="detailed print( -v: info, -vv: debug)",
                        action='count', default=0)
    parser.add_argument("-d", help="the directory to store your data", default='.')
    parser.add_argument("-n", help="the number of concurrent workers (coroutines, actually)",
                        type=int, default=100)
    parser.add_argument("-p", help="max pages, if you know", type=int, default=100)

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
    max_pages = args.p

    d = args.d
    try:
        pathlib.Path(args.d + '/resources').mkdir(parents=True, exist_ok=True)
    except OSError:
        log.error('Failed to create directory: {}'.format(args.d + '/resources'))
        return -1

    log.info('Start fetching {}...'.format(url))

    # TODO: 待改进, 目前获取博客列表和获取每个博客内容这两部分工作还是串行的, 下一步改造成流式处理
    loop = asyncio.get_event_loop()
    blog_list_tasks = [blog_items(url, i) for i in range(max_pages)]
    result = loop.run_until_complete(asyncio.gather(*blog_list_tasks))

    blogs = [item for sublist in result if sublist for item in sublist]
    log.debug('\n\n {} blogs: {}'.format(len(blogs), blogs))

    # Limit the number of coroutines
    sem = asyncio.Semaphore(worker_num)

    content_tasks = [download_blog_item(sem, entry_url, entry_title, entry_date, d)
                     for entry_date, entry_url, entry_title in blogs]

    loop.run_until_complete(asyncio.wait(content_tasks))

    # Retry the failed urls.
    log.info('Try to get the failed urls...')
    failed_tasks = [download_blog_item(sem, entry_url, entry_title, entry_date, d)
                    for entry_date, entry_url, entry_title in failed_url]

    failed_url = []

    loop.run_until_complete(asyncio.wait(failed_tasks))

    log.info('Tried my best.')
    for entry_date, entry_url, entry_title in failed_url:
        log.info('{}  {}  {}'.format(entry_date, entry_url, entry_title))

    loop.close()

    elapsed = int(time.time() - start)

    log.info("Fetched {} blogs in {} seconds, {} others failed. Bye."
             .format(fetched_blog_num, elapsed, len(failed_url)))

    return 0


if __name__ == '__main__':
    sys.exit(main())
