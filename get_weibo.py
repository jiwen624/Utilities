#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Use this utility to retrieve all your weibo (< 2000?).

[Reference]
  1. login protocol: https://github.com/xchaoinfo

"""
import requests
import re
import json
import base64
import time
import math
import random
import getpass
import logging
import random
import argparse
import pathlib
import sys
import copy
from io import BytesIO
from PIL import Image
from urllib.parse import quote_plus

log = logging.getLogger(__name__)


# TODO: 将weibo输出为markdown格式.
# TODO: 2. 可否输完验证码回车后自动把图片文件关闭?(或者预览的方式显示?)
# TODO: 4. 格式化打印(simple) (pandas?).
# TODO: raise its own exception?

class WeiboX:
    """This class can be used to retrieve your Weibo data:
    - tweets
    - comments
    - pictures (TODO)
    """

    def __init__(self,
                 account,  # account name
                 target_uname,
                 target_uid,
                 directory='.',  # the directory to store extracted files
                 interval=2,  # interval between two fetches
                 max_retries=10,  # maximum retries after reaching the 'mod/empty'
                 to_format='simple',  # the file format for storing the data
                 first_n=99999):  # retrieve the first n pages
        """
        The init function.
        :param account: The weibo account name
        :param target_uid: The uid of the target user.
                    (This should be retrieved from some pages but currently not)
        :param directory: The directory used to store retrieved data.
        :param interval: The interval (in seconds) between page retrivals
        :param max_retries: The maximum retry times if weibo returns 'mod_type/empty'
                        It seems that sometimes it's not empty when we got 'mod_type/empty' :(
        """
        # Request agent string
        self.agent = ('Mozilla/5.0 (Windows NT 6.2; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/49.0.2623.110 Safari/537.36')

        # common request headers
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            'User-Agent': self.agent
        }

        self.sess = requests.session()

        # Access the initial page of login.
        # TODO: useless? no data fetched.
        try:
            self.index_url = "https://passport.weibo.cn/signin/login"

            passport_headers = copy.deepcopy(self.headers)
            passport_headers['Host'] = 'passport.weibo.cn'

            self.sess.get(self.index_url, headers=passport_headers)
        except requests.exceptions.RequestException as e:
            log.error('Network failure({}): {}'.format(self.index_url, e))
            raise

        self.dir_str = directory
        d = pathlib.Path(directory)
        try:
            d.mkdir(parents=True, exist_ok=True)
            pathlib.Path(self.dir_str + '/pics').mkdir(parents=True, exist_ok=True)
            pathlib.Path(self.dir_str + '/json').mkdir(parents=True, exist_ok=True)

        except OSError as e:
            log.error('Failed to create directory: {}: {}'.format(directory, e))
            raise

        # use this variable to store the parsed data
        self.weibo = []
        self.account = account  # account name: abc@example.com or cell phone number

        self.password = getpass.getpass('Password:')

        self.uid = ''  # we need to get the uid/cid from the webpage
        self.screen_name = ''
        self.weibo_count = ''

        self.target_uid = target_uid  # this is the user we need to access, might be None here!
        self.target_screen_name = target_uname if target_uname else ''
        self.target_weibo_count = 0

        self.interval = interval
        self.max_retries = max_retries
        self.store_format = to_format
        self.first_n = first_n

    def get_su(self):
        """
        This function is to build the 'su' required by weibo requests.
        :return:
        """
        username_quote = quote_plus(self.account)
        username_base64 = base64.b64encode(username_quote.encode("utf-8"))
        return username_base64.decode("utf-8")

    @staticmethod
    def get_cid(uid):
        return '100505' + uid

    def pre_login(self):
        """
        Redirects before successfully login.
        :return:
        """
        call_back = ("jsonpcallback" +
                     str(int(time.time() * 1000) + math.floor(random.random() * 100000)))
        params = {
            "checkpin": "1",
            "entry": "mweibo",
            "su": self.get_su(),
            "callback": call_back
        }

        pre_url = "https://login.sina.com.cn/sso/prelogin.php"
        prelogin_headers = copy.deepcopy(self.headers)
        prelogin_headers["Host"] = "login.sina.com.cn"
        prelogin_headers["Referer"] = self.index_url

        log.info('Start prelogin to fetch the captcha.')
        try:
            pre = self.sess.get(pre_url, params=params, headers=prelogin_headers)
        except requests.exceptions.RequestException as e:
            log.error('Network failure({}): {}'.format(pre_url, e))
            raise

        pa = r'\((.*?)\)'

        res = re.findall(pa, pre.text)
        if not res:
            log.error(pre.text)
            log.error("Please check the network or your username.")
        else:
            js = json.loads(res[0])

            if js.get("showpin") == 1:  # returns None if 'showpin' doesn't exist.
                captcha_headers = copy.deepcopy(self.headers)

                captcha_headers["Host"] = "passport.weibo.cn"
                captcha_url = 'https://passport.weibo.cn/captcha/image'

                try:
                    capt = self.sess.get(captcha_url, headers=captcha_headers)
                except requests.exceptions.RequestException as e:
                    log.error('Network failure({}): {}'.format(captcha_url, e))
                    raise

                capt_json = capt.json()

                try:
                    capt_base64 = capt_json['data']['image'].split("base64,")[1]
                except KeyError as e:
                    log.error('Failed to fetch captcha: {}'.format(e))
                    log.error('CaptchaJson:{}'.format(capt_json))
                    return ''

                # get captcha here
                im_buff = BytesIO(base64.b64decode(capt_base64))  # TODO: Do I need to close it explicitly?

                im = Image.open(im_buff)
                im.show()
                im.close()
                cha_code = input("Input characters shown to you(请输入图片上的字符):")

                return cha_code, capt_json['data']['pcid']
            else:
                return ''

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

        login_headers = copy.deepcopy(self.headers)
        login_headers["Host"] = "passport.weibo.cn"
        login_headers["Referer"] = self.index_url
        login_headers["Origin"] = "https://passport.weibo.cn"
        login_headers["Content-Type"] = "application/x-www-form-urlencoded"

        post_url = "https://passport.weibo.cn/sso/login"

        try:
            log.info('Sending login request.')
            # print(login_headers)
            login = self.sess.post(post_url, data=post_data, headers=login_headers)
        except requests.exceptions.RequestException as e:
            log.error('Failed to login: {}'.format(e))
            return

        if login.status_code != 200:
            log.error('LoginStatusCode: {}'.format(login.status_code))
            log.error('LoginRsp: {}'.format(login.text))
            raise requests.exceptions.HTTPError('StatusCode of login is not 200')

        js = login.json()
        try:
            uid = js["data"]["uid"]
            log.info('Found uid={}'.format(uid))  # seems we cannot get the user name here.

            self.uid = uid

            cross_domain = js["data"]["crossdomainlist"]
            cn = "https:" + cross_domain["sina.com.cn"]
        except KeyError as e:
            log.error('LoginJsKeyError: {}'.format(e))
            # log.error('LoginRsp: {}'.format(login.text))
            log.error('LoginjsJson: {}'.format(js))
            raise Exception('Login failed: unable to get uid.')

        # TODO: useless?
        cn_headers = copy.deepcopy(self.headers)
        cn_headers["Host"] = "login.sina.com.cn"

        try:
            self.sess.get(cn, headers=cn_headers)
        except requests.exceptions.RequestException as e:
            log.error('Network failure({}): {}'.format(cn, e))
            raise

        # get user name and uid and weibo count
        # first we get this kind of information of the login user.
        ht_headers = copy.deepcopy(self.headers)
        ht_headers["Host"] = "weibo.cn"
        try:
            ht = self.sess.get("http://m.weibo.cn/{}".format(self.uid, headers=ht_headers))
        except requests.exceptions.RequestException as e:
            log.error('Network failure: {}'.format(e))
            raise

        log.debug('ht.url: {}'.format(ht.url))
        log.debug('Session.cookies: {}'.format(self.sess.cookies))

        self.screen_name = re.findall(r'"name":"(.*?)"', ht.text)[0]
        self.weibo_count = re.findall(r'"mblogNum":"(.*?)"', ht.text)[0]

        log.info('{} has been successfully logged in!'.format(self.screen_name))
        # TODO: end of useless code?

        # fetch the uid if the user has speficied the target user name
        if self.target_screen_name:
            tname_headers = copy.deepcopy(self.headers)
            tname_headers["Host"] = "weibo.cn"
            try:
                tname = self.sess.get("http://m.weibo.cn/n/{}".format(self.target_screen_name, headers=tname_headers))
            except requests.exceptions.RequestException as e:
                log.error('Network failure: {}'.format(e))
                raise

            if tname.status_code == 200:
                self.target_uid = re.findall(r'100505(.*?)%', tname.cookies['M_WEIBOCN_PARAMS'])[0]
                self.target_weibo_count = re.findall(r'"mblogNum":"(.*?)"', tname.text)[0]
                log.info('Found target uid: {} name: {} weibo_num: {}'.format(self.target_uid,
                                                                              self.target_screen_name,
                                                                              self.target_weibo_count))
            else:
                raise Exception('Failed to get target user info after login.')

        elif self.target_uid:  # uid is provided
            # get target user name and the weibo count
            tid_headers = copy.deepcopy(self.headers)
            tid_headers["Host"] = "weibo.cn"
            try:
                tid = self.sess.get("http://m.weibo.cn/{}".format(self.target_uid, headers=tid_headers))
            except requests.exceptions.RequestException as e:
                log.error('Network failure: {}'.format(e))
                raise

            if tid.status_code == 200:
                self.target_screen_name = re.findall(r'"name":"(.*?)"', tid.text)[0]
                self.target_weibo_count = re.findall(r'"mblogNum":"(.*?)"', tid.text)[0]
                log.info(
                    'Found target uname: {} weibo_num: {}'.format(self.target_screen_name, self.target_weibo_count))
            else:
                raise Exception('Failed to get target user info after login.')

        else:  # neither target user name nor target uid is assigned, use the login user instead
            self.target_uid = self.uid
            self.target_screen_name = self.screen_name
            self.target_weibo_count = self.weibo_count

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
            log.debug('WeiboItemJson: {}', card)

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

            # Download pictures
            pic_headers = copy.deepcopy(self.headers)
            pic_headers['Host'] = 'ww3.sinaimg.cn'

            for pic_id in pic_ids:
                pic_url = 'http://ww3.sinaimg.cn/large/{}.jpg'.format(pic_id)
                pic_rsp = self.sess.get(pic_url, headers=pic_headers)

                if pic_rsp.status_code == 200:
                    with open('{}/pics/{}.jpg'.format(self.dir_str, pic_id), 'wb') as pic_f:
                        pic_f.write(pic_rsp.content)
                else:
                    log.error('Failed to download {}'.format(pic_url))

            # Extract comments.
            # TODO: just comments, the forwards are not included.
            comments = []
            comments_count = card['mblog']['comments_count']  # this variable will be used to fill out the weibo struct

            if comments_count != 0:
                uid = card['mblog']['user']['id']
                mblog_id = card['mblog']['id']

                cmt_num_fetched = 0
                cmt_page = 1

                with open('{}/json/{}_{}_comment.json'.format(self.dir_str, uid, mblog_id), 'w') as cmt_file:
                    while cmt_num_fetched < comments_count:
                        comment_url = ('http://m.weibo.cn/{}/{}/rcMod?format=cards&type=comment&hot=0&page={}'
                                       .format(uid, mblog_id, cmt_page))
                        log.debug('CommentNum: {} CommentURL: {}'.format(comments_count, comment_url))
                        try:
                            cmt_headers = copy.deepcopy(self.headers)
                            cmt_headers['Host'] = 'm.weibo.cn'
                            cmt_rsp = self.sess.get(comment_url, headers=cmt_headers)
                        except requests.exceptions.RequestException as e:
                            log.error('Network failure({}): {}'.format(comment_url, e))
                            raise

                        cmt_rsp.encoding = 'utf-8'
                        cmt_file.write(cmt_rsp.text)
                        log.debug('CommentRspText: {}'.format(cmt_rsp.text))

                        comments_json = json.loads(cmt_rsp.text)
                        log.debug('CommentsJson: {}'.format(comments_json))

                        comment_page_mode = comments_json[0].get('mod_type')
                        if comment_page_mode == 'mod/pagelist':
                            new_comments = self.parse_comments(comments_json)

                            # fill out the comments struct
                            comments.extend(new_comments)

                            cmt_num_fetched += len(new_comments)
                            cmt_page += 1

                        elif comment_page_mode == 'mod/empty':
                            log.info('Comments num: {}, fetched: {}'.format(comments_count, cmt_num_fetched))
                            break

                        else:
                            log.error('Unexpected comment mod: {}'.format(comment_page_mode))
                            break

            else:  # no comments
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
        """To store the weibo into a file: txt, csv or other formats.
        :param :
        :return:
        """
        # We probably don't need to check if self.weibo is empty: it works normally when self.weibo=[]
        if not len(self.weibo):
            log.error('Serialization invoked but no weibo entries found!')
            return

        if self.store_format == 'simple':
            self.to_simple()
        elif self.store_format == 'csv':
            self.to_csv()
        elif self.store_format == 'markdown':
            self.to_markdown()
        else:
            log.error('Unsupported format: {}'.format(self.store_format))
            pass  # TODO: other formats are currently not supported.

    def to_csv(self):
        """
        This function stores the weibo data into a csv file.
        The pictures are stored in the same directory with the name of 'pic_id'.jpg
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
        :return:
        """
        log.error('Unsupported format: {}'.format(self.store_format))

    def to_markdown(self):
        """
        This function stores the weibo data into a markdown file.
        The pictures are stored in the same directory with the name of 'pic_id'.jpg
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
        :return:
        """
        log.error('Unsupported format: {}'.format(self.store_format))

    def to_simple(self):
        """
        This function stores the weibo data into a simple text file.
        The pictures are stored in the same directory with the name of 'pic_id'.jpg
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
        :return:
        """
        log.info('Storing data: {} in total, {} of them fetched'.format(self.target_weibo_count, len(self.weibo)))

        weibo_file_name = '{}/{}.txt'.format(self.dir_str, self.weibo[0]['screen_name'])

        with open(weibo_file_name, 'w') as f:
            print('截至目前, {}发了{}条微博:'.format(self.target_screen_name, self.target_weibo_count), file=f)

            # print every weibo item.
            for idx, item in enumerate(self.weibo, start=1):

                # 0. print the title
                print('[{}]{}'.format(idx, '-' * 100), file=f)
                print('@{}    评论数:({})      发布日期: {}    来自: {}'.format(
                    item['screen_name'],
                    item['comments_count'],  # Number of comments
                    item['created_at'][2:],  # Drop the first two digits of yyyy
                    item['source']
                ), file=f)

                # 1. print the weibo text
                print('{}'.format(item['text']), file=f)  # Print to a file instead of stdout

                # 2. print the forwarded weibo, if any
                # {
                #     'text': txt,
                #     'screen_name': card['mblog']['retweeted_status']['user']['screen_name'],
                #     'pic_ids': card['mblog']['retweeted_status'].get('pic_ids')
                # }
                if item['retweet_item']:
                    print('{}转发: @{}: {}\n'.format('>> ',  # leave a line between weibo and its comments
                                                   item['retweet_item']['screen_name'],
                                                   item['retweet_item']['text']),
                          file=f)

                # 3. print comments.
                if item['comments_count'] != 0:
                    for comment in item['comments']:
                        print('{}|- @{}: {}'.format(' ' * 10, comment['screen_name'], comment['text']),
                              file=f)

                # 4. print picture ids (which will be downloaded into directory 'pic'
                if item['pic_ids']:
                    print('Pics: {}'.format(item['pic_ids']), file=f)

                # Leave a line between weibo items.
                print('', file=f)

        log.info('Check path: {} for your weibo records.'.format(weibo_file_name))

    def fetch_tweets(self):
        """The main function.
        :return:
        """
        try:
            # Simulated prelogin.
            pin_code = self.pre_login()

            # And login.
            self.login(pin_code)
        except Exception as e:
            log.error('Login failed: {}'.format(e))
            raise

        eop_retry = 0  # Retry times after the end of pages (Disable it)
        curr_page_idx = 1

        log.info('Start fetching...')

        # get target uid (then get cid from uid):
        # if we've specified the target user, then target_uid=(uid of that user)
        # else target_uid=(the uid of the user whom we used to login)
        # - we've already got self.uid when logged in
        page_headers = copy.deepcopy(self.headers)

        target_uid = self.target_uid if self.target_uid else self.uid
        target_cid = self.get_cid(target_uid)

        page_headers['Host'] = 'm.weibo.cn'
        page_headers['Accept'] = 'application/json, text/javascript, */*; q=0.01'
        page_headers['X-Requested-With'] = 'XMLHttpRequest'
        page_headers['Referer'] = ('http://m.weibo.cn/page/tpl?containerid={}_-_WEIBO_SECOND_PROFILE_WEIBO'
                                   .format(target_cid))
        page_headers['Accept-Encoding'] = 'gzip, deflate, sdch'
        page_headers['Accept-Language'] = 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4'

        # start to fetch weibo pages
        while True:
            # sleep for a while if error happened
            time.sleep(10*eop_retry)

            try:
                rsp = self.sess.get('http://m.weibo.cn/page/json?containerid={}_-_WEIBO_SECOND_PROFILE_WEIBO&page={}'
                                    .format(target_cid, curr_page_idx), headers=page_headers)
            except requests.exceptions.RequestException as e:
                log.error('Network failure: {}'.format(e))
                raise

            # r.encoding = 'utf-8'
            if rsp.status_code != 200:
                log.error('GetPageRspCode: {}'.format(rsp.status_code))
                log.error('WeiboPageText: ', rsp.text)

                eop_retry += 1
                if eop_retry >= self.max_retries:
                    break
                else:
                    log.info('Retry: {}'.format(eop_retry))
                    continue

            # It returns either an html or json string.
            if rsp.text.startswith('<!doctype html>'):
                json_str = re.findall(r'window\.\$render_data = (.*?);</script>', rsp.text, flags=re.S)[0]
                log.debug('JsonStrInWeiboRsp: {}'.format(json_str))
                page = json.loads(json_str)
            else:
                page = json.loads(rsp.text)

            # Check if we've reached the end by examine the mod_type.
            try:
                mod_type = page['cards'][0].get('mod_type')
            except KeyError:  # the json returned by server may not contain 'card' or 'mod_type'
                log.warning('Request reset by server? {}'.format(page.get('msg')))
                log.debug('And the PageJson: {}'.format(page))
                mod_type = None

            # different mod_type means different action: empty->the end; pagelist->continue; others->error!
            if mod_type == 'mod/empty':
                log.info('Reaching the end? Try it again: {}'.format(eop_retry))
                eop_retry += 1  # we'd like to try a couple of more times to make sure we're reaching the end.
                if eop_retry >= self.max_retries:  # Maximum retry times
                    break

            elif mod_type == 'mod/pagelist':
                with open('{}/json/uid{}_{}.json'.format(self.dir_str, self.target_uid, curr_page_idx), 'w') as f:
                    print(rsp.text, file=f)
                    self.weibo.extend(self.parse_page(page))  # Invoke page parser here.

                eop_retry = 0  # Reset retry times

                log.info(' - The page {} is done.'.format(curr_page_idx))
                curr_page_idx += 1
                if curr_page_idx >= self.first_n:
                    log.info('Exiting after reaching the maximum pages allowed: {}'.format(self.first_n))
                    break
            else:
                log.warning('Bad mod_type: {}. Try it again: {}'.format(mod_type, eop_retry))
                eop_retry += 1
                if eop_retry >= self.max_retries:  # Maximum retry times
                    break

            # random interval between each fetch.
            time.sleep(random.randrange(self.interval, self.interval * 5))

            # hardcoded, let's take a break to avoid being banned.
            if not curr_page_idx % 10:
                time.sleep(curr_page_idx)

        if eop_retry == self.max_retries:
            log.warning('Give up after {} retries.'.format(eop_retry))

        self.serialize()
        log.info('-That\'s it-')


def validate_uid(s):
    if len(s) == 10:
        return s
    else:
        msg = "uid must be a string with 10 digits: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def main():
    """The main function to retrieve weibo.
    """
    parser = argparse.ArgumentParser(description="The Utility to backup your weibo :P")
    parser.add_argument("name", help="your weibo account name")

    parser.add_argument("-v", help="detailed print( -v: info, -vv: debug)",
                        action='count', default=0)
    parser.add_argument("-d", help="the directory to store your data")
    parser.add_argument("-u", help="target user name")
    parser.add_argument("-i", help="the target uid, which will be ignored if you've specified -u",
                        type=validate_uid)

    parser.add_argument("-n", help="fetch the first n pages.", type=int, default=99999)
    parser.add_argument("-m", help="maximum number of retries.", type=int, default=10)
    parser.add_argument("-t", help="base interval between each fetch.", type=int, default=2)

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
    target_uname = args.u if args.u else ''
    target_uid = args.i if args.i else ''
    max_retries = args.m
    interval = args.t

    # This is a sample:
    # 1. Instantiate the WeiboX class with mandatory parameters
    # 2. Call WeiboX.fetch_tweets()
    # 3. Check the files under current directory (or other specified directory)
    w = WeiboX(account=username,
               target_uname=target_uname,
               target_uid=target_uid,
               max_retries=max_retries,
               interval=interval,
               directory=dir_name,
               to_format=to_format,
               first_n=first_n)

    # try:
    w.fetch_tweets()
    # except Exception as e:
    #     log.error('Failed to fetch weibo items. {}'.format(e))
    #     return 1

    return 0


# Run: ./get_weibo.py name cid -v
if __name__ == "__main__":
    sys.exit(main())
