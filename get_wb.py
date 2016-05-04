#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import re
import json
import base64
import time
import math
import random
import getpass
import logging
import argparse
import pathlib
import sys
from PIL import Image
try:
    from urllib.parse import quote_plus
except:
    from urllib import quote_plus


# 构造 Request headers
agent = 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.110 Safari/537.36'
global headers
headers = {
    "Host": "passport.weibo.cn",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    'User-Agent': agent
}

dir_str = '.'

session = requests.session()
# 访问登录的初始页面
index_url = "https://passport.weibo.cn/signin/login"
session.get(index_url, headers=headers)


def get_su(username):
    """
    对 email 地址和手机号码 先 javascript 中 encodeURIComponent
    对应 Python 3 中的是 urllib.parse.quote_plus
    然后在 base64 加密后decode
    """
    username_quote = quote_plus(username)
    username_base64 = base64.b64encode(username_quote.encode("utf-8"))
    return username_base64.decode("utf-8")


def login_pre(username):
    # 采用构造参数的方式
    params = {
        "checkpin": "1",
        "entry": "mweibo",
        "su": get_su(username),
        "callback": "jsonpcallback" + str(int(time.time() * 1000) + math.floor(random.random() * 100000))
    }

    pre_url = "https://login.sina.com.cn/sso/prelogin.php"
    headers["Host"] = "login.sina.com.cn"
    headers["Referer"] = index_url
    pre = session.get(pre_url, params=params, headers=headers)
    pa = r'\((.*?)\)'
    res = re.findall(pa, pre.text)
    if res == []:
        logging.error("Please check the network or your username.")
    else:
        js = json.loads(res[0])
        if js["showpin"] == 1:
            headers["Host"] = "passport.weibo.cn"
            capt = session.get("https://passport.weibo.cn/captcha/image", headers=headers)
            capt_json = capt.json()
            capt_base64 = capt_json['data']['image'].split("base64,")[1]
            with open('capt.jpg', 'wb') as f:
                f.write(base64.b64decode(capt_base64))
                f.close()
            im = Image.open("capt.jpg")
            im.show()
            im.close()
            cha_code = input("Input the captcha(验证码图片已打开,请输入图片上的数字):\n>")
            return cha_code, capt_json['data']['pcid']
        else:
            return ""


def login(username, password, pincode):
    postdata = {
        "username": username,
        "password": password,
        "savestate": "1",
        "ec": "0",
        "pagerefer": "",
        "entry": "mweibo",
        "wentry": "",
        "loginfrom": "",
        "client_id": "",
        "code": "",
        "qq": "",
        "hff": "",
        "hfp": "",
    }
    if pincode == "":
        pass
    else:
        postdata["pincode"] = pincode[0]
        postdata["pcid"] = pincode[1]

    headers["Host"] = "passport.weibo.cn"
    headers["Reference"] = index_url
    headers["Origin"] = "https://passport.weibo.cn"
    headers["Content-Type"] = "application/x-www-form-urlencoded"

    post_url = "https://passport.weibo.cn/sso/login"
    login = session.post(post_url, data=postdata, headers=headers)
    logging.debug('Cookies: ', login.cookies)
    logging.debug('LoginStatusCode:{}'.format(login.status_code))
    js = login.json()
    logging.debug('js: {}'.format(js))

    uid = js["data"]["uid"]
    crossdomain = js["data"]["crossdomainlist"]
    cn = "https:" + crossdomain["sina.com.cn"]
    # 下面两个对应不同的登录 weibo.com 还是 m.weibo.cn
    # 一定要注意更改 Host
    # mcn = "https:" + crossdomain["weibo.cn"]
    # com = "https:" + crossdomain['weibo.com']
    headers["Host"] = "login.sina.com.cn"
    session.get(cn, headers=headers)
    headers["Host"] = "weibo.cn"
    ht = session.get("http://weibo.cn/{}/info".format(uid, headers=headers))
    logging.debug('ht.url: {}'.format(ht.url))
    logging.debug('Session.cookies: {}'.format(session.cookies))
    pa = r'<title>(.*?)</title>'
    res = re.findall(pa, ht.text)
    logging.info("Login successful!")

    return


def _remove_tags(text):
    """Remove the html tags of the text
    :param text:
    :return:
    """
    return re.sub(r'<.*?>', '', text)


def parse_comments(comments_json):
    """Parse the comments (if any) of the weibo items.
    :param comments_json:
    :return:
    """
    if not comments_json:
        logging.error('Comment parser invoked but no comments found.')
        return None

    comments = []

    for cmt in comments_json[0]['card_group']:
        screen_name = cmt['user']['screen_name']
        text = _remove_tags(cmt['text'])

        comments.append(
            {'screen_name': screen_name,
             'text': text
             }
        )

    return comments


def parse_page(page_json):
    """Parse the weibo page (there are many of them.)
    :param page_json:
    :return:
    """
    global dir_str

    if not page_json:
        logging.error('Page parser invoked but no weibo items found.')
        return None

    weibo_list = []

    for card in page_json['cards'][0]['card_group']:
        logging.debug('WeiboJson: ', card)

        weibo_item = card.get('mblog')
        if weibo_item is None:
            # The first item may be the 'search weibo' at the top of the screen - just skip it.
            continue
        else:
            weibo_id = weibo_item['id']

        text = _remove_tags(card['mblog']['text'])
        logging.debug('NewText: {}\n'.format(text))

        created_at = card['mblog']['created_at']
        pic_ids = card['mblog']['pic_ids']  # url: http://ww3.sinaimg.cn/large/{pic_id}.jpg
        comments_num = card['mblog']['comments_count']
        source = card['mblog']['source']
        screen_name = card['mblog']['user']['screen_name']

        # Extract forwarded weibo.
        if card['mblog'].get('retweeted_status'):
            logging.debug('Retweeted: {}'.format(card['mblog'].get('retweeted_status')))

            txt = _remove_tags(card['mblog']['retweeted_status']['text'])
            retweet_item = {
                'text': txt,
                'screen_name': card['mblog']['retweeted_status']['user']['screen_name'],
                'pic_ids': card['mblog']['retweeted_status'].get('pic_ids')
            }
        else:
            retweet_item = None

        # Extract comments.
        # TODO: forwards not included.
        comments = []

        if card['mblog']['comments_count'] != 0:
            uid = card['mblog']['user']['id']
            mblog_id = card['mblog']['id']

            cmt_num = 0
            with open('{}/{}_{}_comment.json'.format(dir_str, uid, mblog_id), 'w') as cmt_f:
                while cmt_num < card['mblog']['comments_count']:
                    # url got: http://m.weibo.cn/1736347302/3517371904252336/rcMod?format=cards&type=comment&hot=1
                    comment_url = 'http://m.weibo.cn/{}/{}/rcMod?format=cards&type=comment&hot=1'.format(uid, mblog_id)
                    logging.debug('CommentNum: {} CommentURL: {}'.format(card['mblog']['comments_count'], comment_url))
                    cmt_r = session.get(comment_url, headers=headers)

                    cmt_r.encoding = 'utf-8'
                    cmt_f.write(cmt_r.text)
                    comments_json = json.loads(cmt_r.text)

                    if comments_json[0]['mod_type'] == 'mod/empty':
                        break

                    new_comments = parse_comments(comments_json)
                    comments.extend(new_comments)
                    cmt_num += len(new_comments)

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
                'comments_num': comments_num,
                'source': source,
                'screen_name': screen_name,
                'retweet_item': retweet_item,
                'comments': comments
             }
        )

    return weibo_list

# TODO: 以表格方式打印(有现成的库), comments缩进8 spaces
def serialize(weibo):
    if not len(weibo):
        logging.error('Serialization invoked but no weibo entries found!')
        return

    with open('{}/{}.txt'.format(dir_str, weibo[0]['screen_name']), 'w') as f:
        for item in weibo:
            print('{}    {}'.format(item['created_at'], item['text']), file=f)

            # Print comments.
            if item['comments_num'] != 0:
                for comment in item['comments']:
                    print('    - @{}: {}'.format(comment['screen_name'], comment['text']), file=f)


def main()
    """The main function.
    :return:
    """

    global dir_str
    weibo = []
    parser = argparse.ArgumentParser(description="The Utility to backup your weibo :P")
    parser.add_argument("-v", help="detailed print( -v: info, -vv: debug)", action='count', default=0)
    parser.add_argument("-d", help="the directory to store your data")

    args = parser.parse_args()

    if args.v == 0:
        log_level = logging.ERROR
    elif args.v == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    logging.basicConfig(
        filename='/dev/stdout',
        level=log_level,
        format='%(message)s'
        # format='%(levelname)s:%(asctime)s:%(message)s'
    )

    # username = input('Your user name:')
    # password = getpass.getpass('Password')
    # container_id = input('Your container id :(')
    username = 'grantte@gmail.com'
    password = '821030'
    container_id = '1005051736347302'

    dir_str = args.d if args.d else username
    dir = pathlib.Path(dir_str)
    try:
        dir.mkdir(exist_ok=True)
    except:
        logging.error('Failed to create directory: {}, exiting...'.format(dir_str))
        return 1

    pin_code = login_pre(username)
    login(username, password, pin_code)

    eop_retry = 0  # Retry times after the end of pages (Disable it)
    curr_page_idx = 1

    while True:
        headers['Host'] = 'm.weibo.cn'
        headers['Accept'] = 'application/json, text/javascript, */*; q=0.01'
        headers['X-Requested-With'] = 'XMLHttpRequest'
        headers['Referer'] = 'http://m.weibo.cn/page/tpl?containerid={}_-_WEIBO_SECOND_PROFILE_WEIBO'.format(container_id)
        headers['Accept-Encoding'] = 'gzip, deflate, sdch'
        headers['Accept-Language'] = 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4'

        r = session.get('http://m.weibo.cn/page/json?containerid={}_-_WEIBO_SECOND_PROFILE_WEIBO&page={}'
                        .format(container_id, curr_page_idx), headers=headers)
        # r.encoding = 'utf-8'
        logging.debug('WeiboPageRsp: ', r.text)

        # It returns either an html or json string.
        if r.text.startswith('<!doctype html>'):
            json_str = re.findall(r'window\.\$render_data = (.*?);</script>', r.text, flags=re.S)[0]
            logging.debug('JsonStrInWeiboRsp: {}'.format(json_str))
            page = json.loads(json_str)
        else:
            page = json.loads(r.text)

        if page['cards'][0]['mod_type'] == 'mod/empty':
            logging.info('Reaching the end? Try it again')
            eop_retry += 1
            if eop_retry >= 3: # Maximum retry times: 3
                break
        else:
            with open('{}/{}_{}.json'.format(dir_str, username, curr_page_idx), 'w') as f:
                print(r.text, file=f)

            weibo.extend(parse_page(page))
            eop_retry = 0

            logging.info('The page {} is done.'.format(curr_page_idx))
            curr_page_idx += 1

        time.sleep(2)

    serialize(weibo)

    logging.info('-EOF-')
    return 0


if __name__ == "__main__":
    sys.exit(main())
