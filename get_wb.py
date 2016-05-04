#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import re
import json
import base64
import time
import math
import random
# import getpass
import logging
import argparse
import pathlib
import sys
from PIL import Image
from urllib.parse import quote_plus

log = logging.getLogger(__name__)

# TODO: 0. 给自己的账号, 爬另一个人的weibo.
# TODO: 1. 登陆前后多打印写信息(等待时间太长)
# TODO: 2. 可否输完验证码回车后自动把图片文件关闭?(或者预览的方式显示?)
# TODO: 3. 每个page的间隔时间设置成2-10s的随机数
# TODO: 4. 格式化打印(simple) (pandas? 好像不太好搞.
# TODO: 5. 下载微博图片以及转发的微博的图片(有可能不止一张!)
# TODO: use requests.json() to replace json module?
# TODO: raise its own exception?

class WeiboX:
    """This class can be used to retrieve your Weibo data:
    - tweets
    - comments
    - pictures (TODO)
    """

    def __init__(self, account, cid=0, directory='.', interval=2, retries=3, to_format='simple', first_n=99999):
        """
        The init function.
        :param account: The weibo account name
        :param cid: The container id of this user.
                    (This should be retrieved from some pages but currently not)
        :param directory: The directory used to store retrieved data.
        :param interval: The interval (in seconds) between page retrivals
        :param retries: The maximum retry times if weibo returns 'mod_type/empty'
                        It seems that sometimes it's not empty when we got 'mod_type/empty' :(
        """
        # Request agent string
        self.agent = 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) ' \
                     'AppleWebKit/537.36 (KHTML, like Gecko) ' \
                     'Chrome/49.0.2623.110 Safari/537.36'

        # Request headers
        self.headers = {
            "Host": "passport.weibo.cn",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            'User-Agent': self.agent
        }

        self.sess = requests.session()

        # Access the initial page of login.
        try:
            self.index_url = "https://passport.weibo.cn/signin/login"
            self.sess.get(self.index_url, headers=self.headers)
        except requests.exceptions.RequestException as e:
            log.error('Network failure({}): {}'.format(self.index_url, e))
            raise

        self.dir_str = directory
        d = pathlib.Path(directory)
        try:
            d.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            log.error('Failed to create directory: {}: {}'.format(directory, e))
            raise

        self.weibo = []

        # password = getpass.getpass('Password')

        self.account = account

        self.password = '821030'
        self.container_id = cid  # cid = '100505' + uid
        # self.container_id = container_id

        self.interval = interval
        self.max_retries = retries
        self.to_format = to_format
        self.first_n = first_n

    def get_su(self):
        """
        This function is to build the 'su' required by weibo requests.
        :return:
        """
        username_quote = quote_plus(self.account)
        username_base64 = base64.b64encode(username_quote.encode("utf-8"))
        return username_base64.decode("utf-8")

    def pre_login(self):
        """
        Redirects before successfully login.
        :return:
        """
        call_back = "jsonpcallback" \
                    + str(int(time.time() * 1000) + math.floor(random.random() * 100000))
        params = {
            "checkpin": "1",
            "entry": "mweibo",
            "su": self.get_su(),
            "callback": call_back
        }

        pre_url = "https://login.sina.com.cn/sso/prelogin.php"
        self.headers["Host"] = "login.sina.com.cn"
        self.headers["Referer"] = self.index_url

        log.info('Start prelogin to fetch the captcha.')
        try:
            pre = self.sess.get(pre_url, params=params, headers=self.headers)
        except requests.exceptions.RequestException as e:
            log.error('Network failure({}): {}'.format(pre_url, e))
            raise

        pa = r'\((.*?)\)'

        res = re.findall(pa, pre.text)
        if not res:
            log.error("Please check the network or your username.")
        else:
            js = json.loads(res[0])
            if js["showpin"] == 1:
                self.headers["Host"] = "passport.weibo.cn"
                captcha_url = 'https://passport.weibo.cn/captcha/image'

                try:
                    capt = self.sess.get(captcha_url, headers=self.headers)
                except requests.exceptions.RequestException as e:
                    log.error('Network failure({}): {}'.format(captcha_url, e))
                    raise

                capt_json = capt.json()
                capt_base64 = capt_json['data']['image'].split("base64,")[1]

                with open('capt.jpg', 'wb') as f:
                    f.write(base64.b64decode(capt_base64))
                    f.close()

                im = Image.open("capt.jpg")
                im.show()
                im.close()
                cha_code = input("Input characters shown on the captcha(请输入图片上的字符):")
                return cha_code, capt_json['data']['pcid']
            else:
                return ""

    def login(self, pincode):
        """
        The method to login weibo.
        :param pincode:
        :return:
        """
        post_data = {
            'username': self.account,
            'password': self.password,
            'savestate': '1',
            'ec': '0',
            'pagerefer': '',
            'entry': 'mweibo',
            'wentry': '',
            'loginfrom': '',
            'client_id': '',
            'code': '',
            'qq': '',
            'hff': '',
            'hfp': '',
        }
        if pincode == '':
            pass
        else:
            post_data["pincode"] = pincode[0]
            post_data["pcid"] = pincode[1]

        self.headers["Host"] = "passport.weibo.cn"
        self.headers["Reference"] = self.index_url
        self.headers["Origin"] = "https://passport.weibo.cn"
        self.headers["Content-Type"] = "application/x-www-form-urlencoded"

        post_url = "https://passport.weibo.cn/sso/login"

        try:
            log.info('Sending login request.')
            login = self.sess.post(post_url, data=post_data, headers=self.headers)
        except requests.exceptions.RequestException as e:
            log.error('Failed to login: {}'.format(e))
            return

        log.debug('Cookies: ', login.cookies)
        log.info('LoginStatusCode: {}'.format(login.status_code))

        if login.status_code != '200':
            pass  # TODO: We should raise an exception here.
        js = login.json()
        try:
            uid = js["data"]["uid"]
            log.info('Found uid={}'.format(uid))

            # containerid = '100505' + uid
            self.container_id = '100505{}'.format(uid)

            cross_domain = js["data"]["crossdomainlist"]
            cn = "https:" + cross_domain["sina.com.cn"]
        except KeyError as e:
            log.error('{}'.format(e))
            log.debug('LoginRsp: {}'.format(login.text))
            log.error('LoginjsJson: {}'.format(js))
            return  # TODO: or raise?

        self.headers["Host"] = "login.sina.com.cn"

        try:
            self.sess.get(cn, headers=self.headers)
        except requests.exceptions.RequestException as e:
            log.error('Network failure({}): {}'.format(cn, e))
            raise

        self.headers["Host"] = "weibo.cn"
        try:
            ht = self.sess.get("http://weibo.cn/{}/info".format(uid, headers=self.headers))
        except requests.exceptions.RequestException as e:
            log.error('Network failure: {}'.format(e))
            raise

        log.debug('ht.url: {}'.format(ht.url))
        log.debug('Session.cookies: {}'.format(self.sess.cookies))

        display_name = re.findall(r'<title>(.*?)</title>', ht.text)[0]
        log.info('Got {}, which means you\'ve successfully logged in!'.format(display_name))

        return

    @staticmethod
    def remove_tags(text):
        """Remove the html tags of the text
        :param text:
        :return:
        """
        return re.sub(r'<.*?>', '', text)

    def parse_comments(self, comments_json):
        """Parse the comments (if any) of the weibo items.
        :param comments_json: The json struct to be parsed
        :return: the dict that contains comments elements.
        """
        if not comments_json:
            log.error('Comment parser invoked but no comments found.')
            return None

        comments = []

        for cmt in comments_json[0]['card_group']:
            screen_name = cmt['user']['screen_name']
            text = self.remove_tags(cmt['text'])

            comments.append(
                {'screen_name': screen_name,
                 'text': text
                 }
            )

        return comments

    def parse_page(self, page_json):
        """Parse the weibo page (there are many of them.)
           Invoke this function for each fetched page.
        :param page_json:
        :return:
        """
        if not page_json:
            log.error('Page parser invoked but no weibo items found.')
            return None

        weibo_list = []

        # Each 'card' is a weibo.
        for card in page_json['cards'][0]['card_group']:
            log.debug('WeiboItemJson: ', card)

            # Extract weibo: text, account, time, etc.
            weibo_item = card.get('mblog')
            if weibo_item is None:
                # The first item may be the 'search weibo' at the top of the screen
                # - just skip it.
                continue
            else:
                weibo_id = weibo_item['id']

            text = self.remove_tags(card['mblog']['text'])
            log.debug('NewText: {}\n'.format(text))

            created_at = card['mblog']['created_at']
            # url: http://ww3.sinaimg.cn/large/{pic_id}.jpg
            pic_ids = card['mblog']['pic_ids']
            comments_count = card['mblog']['comments_count']
            source = card['mblog']['source']
            screen_name = card['mblog']['user']['screen_name']

            # Extract the weibo which is forwarded by you.
            if card['mblog'].get('retweeted_status'):
                log.debug('Retweeted: {}'.format(card['mblog'].get('retweeted_status')))

                txt = self.remove_tags(card['mblog']['retweeted_status']['text'])
                retweet_item = {
                    'text': txt,
                    'screen_name': card['mblog']['retweeted_status']['user']['screen_name'],
                    'pic_ids': card['mblog']['retweeted_status'].get('pic_ids')
                }
            else:
                retweet_item = None

            # Extract comments.
            # TODO: just comments, the forwards are not included.
            comments = []

            if card['mblog']['comments_count'] != 0:
                uid = card['mblog']['user']['id']
                mblog_id = card['mblog']['id']

                cmt_num = 0
                cmt_page = 1

                with open('{}/{}_{}_comment.json'.format(self.dir_str, uid, mblog_id), 'w') as cmt_file:
                    while cmt_num < card['mblog']['comments_count']:
                        # url:http://m.weibo.cn/1736347302/3517371904252336/rcMod?format=cards&type=comment&hot=1
                        comment_url = 'http://m.weibo.cn/{}/{}/rcMod?format=cards&type=comment&hot=0&page={}' \
                            .format(uid, mblog_id, cmt_page)
                        log.debug('CommentNum: {} CommentURL: {}'
                                  .format(card['mblog']['comments_count'], comment_url))
                        try:
                            cmt_rsp = self.sess.get(comment_url, headers=self.headers)
                        except requests.exceptions.RequestException as e:
                            log.error('Network failure({}): {}'.format(comment_url, e))
                            raise

                        cmt_rsp.encoding = 'utf-8'
                        cmt_file.write(cmt_rsp.text)
                        comments_json = json.loads(cmt_rsp.text)

                        if comments_json[0]['mod_type'] == 'mod/empty':
                            break

                        new_comments = self.parse_comments(comments_json)
                        comments.extend(new_comments)
                        cmt_num += len(new_comments)
                        cmt_page += 1

            else:
                comments = []

            # TODO: Extract pictures.
            # TODO: Extract forwarded pictures

            weibo_list.append(
                {
                    'weibo_id': weibo_id,
                    'text': text,
                    'created_at': created_at,
                    'pic_ids': pic_ids,
                    'comments_count': comments_count,
                    'source': source,
                    'screen_name': screen_name,
                    'retweet_item': retweet_item,
                    'comments': comments
                }
            )

        return weibo_list

    def serialize(self):
        """
        To store the weibo into a file: txt, csv or other formats.
        This function will parse the following dict got from 'parse_page()':
            {
                'weibo_id': weibo_id,
                'text': text,
                'created_at': created_at,
                'pic_ids': pic_ids,
                'comments_count': comments_count,
                'source': source,
                'screen_name': screen_name,
                'retweet_item': retweet_item,
                'comments': comments
            }

        :param :
        :return:
        """
        if not len(self.weibo):
            log.error('Serialization invoked but no weibo entries found!')
            return

        if self.to_format == 'simple':
            with open('{}/{}.txt'.format(self.dir_str, self.weibo[0]['screen_name']), 'w') as f:
                # Print weibo items.
                for item in self.weibo:
                    print('@{}    Comments:({})      Post Date: {}'.format(
                        item['screen_name'],
                        item['comments_count'],  # Number of comments
                        item['created_at'][2:]  # Drop the first two digits of yyyy
                    ), file=f)

                    print('{}'.format(item['text']), file=f)  # Print to a file instead of stdout

                    # Print comments.
                    if item['comments_count'] != 0:
                        for comment in item['comments']:
                            print('{}- @{}: {}'.format(' ' * 10, comment['screen_name'], comment['text']),
                                  file=f)

                    print('', file=f)  # Leave a line between weibo items.
        else:
            pass  # TODO: other formats are currently not supported.

    def fetch_tweets(self):
        """The main function.
        :return:
        """
        # Simulated prelogin.
        pin_code = self.pre_login()

        # And login.
        self.login(pin_code)

        eop_retry = 0  # Retry times after the end of pages (Disable it)
        curr_page_idx = 1

        log.info('Start fetching...')
        while True:
            self.headers['Host'] = 'm.weibo.cn'
            self.headers['Accept'] = 'application/json, text/javascript, */*; q=0.01'
            self.headers['X-Requested-With'] = 'XMLHttpRequest'
            self.headers['Referer'] = 'http://m.weibo.cn/page/tpl?containerid={}_-_WEIBO_SECOND_PROFILE_WEIBO' \
                .format(self.container_id)
            self.headers['Accept-Encoding'] = 'gzip, deflate, sdch'
            self.headers['Accept-Language'] = 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4'

            try:
                rsp = self.sess.get('http://m.weibo.cn/page/json?containerid={}_-_WEIBO_SECOND_PROFILE_WEIBO&page={}'
                                    .format(self.container_id, curr_page_idx), headers=self.headers)
            except requests.exceptions.RequestException as e:
                log.error('Network failure: {}'.format(e))
                raise

            # r.encoding = 'utf-8'
            log.debug('WeiboPageRsp: ', rsp.text)

            # It returns either an html or json string.
            if rsp.text.startswith('<!doctype html>'):
                json_str = re.findall(r'window\.\$render_data = (.*?);</script>', rsp.text, flags=re.S)[0]
                log.debug('JsonStrInWeiboRsp: {}'.format(json_str))
                page = json.loads(json_str)
            else:
                page = json.loads(rsp.text)

            if page['cards'][0]['mod_type'] == 'mod/empty':
                log.info('Reaching the end? Try it again: {}'.format(eop_retry))
                eop_retry += 1
                if eop_retry >= self.max_retries:  # Maximum retry times
                    break
            else:
                with open('{}/{}_{}.json'.format(self.dir_str, self.account, curr_page_idx), 'w') as f:
                    print(rsp.text, file=f)
                    self.weibo.extend(self.parse_page(page))  # Invoke page parser here.

                eop_retry = 0  # Reset retry times

                log.info(' - The page {} is done.'.format(curr_page_idx))
                curr_page_idx += 1
                if curr_page_idx > self.first_n:
                    log.info('Reaching the upper limit of {} pages. Bye.'.format(self.first_n))
                    break

            time.sleep(self.interval)

        self.serialize()

        log.info('-That\'s it-')


def main():
    """The main function to retrieve weibo.
    """
    parser = argparse.ArgumentParser(description="The Utility to backup your weibo :P")
    parser.add_argument("name", help="your weibo account name")

    parser.add_argument("-v", help="detailed print( -v: info, -vv: debug)",
                        action='count', default=0)
    parser.add_argument("-d", help="the directory to store your data")
    parser.add_argument("-n", help="Fetch the first n pages.", type=int, default=99999)

    parser.add_argument("-f", help="which format you'd like to store your weibo:",
                        choices=['simple', 'csv', 'doc'],
                        default='simple')

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

    username = args.name
    dir_name = args.d if args.d else username

    to_format = args.f
    # TODO: remove these lines after 'csv' and 'doc' are implemented.
    if to_format != 'simple':
        print('Oops! The option {} has not been implemented. :('.format(to_format))
        to_format = 'simple'

    first_n = args.n  # Fetch the first n pages. args.n == 0 means all.

    # This is a sample:
    # 1. Instantiate the WeiboX class with mandatory parameters
    # 2. Call WeiboX.fetch_tweets()
    # 3. Check the files under current directory (or other specified directory)
    w = WeiboX(account=username,
               directory=dir_name,
               to_format=to_format,
               first_n=first_n)

    return w.fetch_tweets()


# Run: ./get_weibo.py name cid -v
if __name__ == "__main__":
    sys.exit(main())
