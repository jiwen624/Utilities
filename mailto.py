#!/usr/bin/env python3
"""
Send emails from command line (I use it for sending attachments directly from a server. :P)
Usage: see ./mailto -h
"""

import os
import sys
import getpass
import smtplib
import argparse
from email import encoders
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
from email.mime.base import MIMEBase

try:
    from pprint import pprint as _print
except ImportError:
    _print = print


def format_header(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))


def send_mail(args):
    """Send emails.

    :param args:
    - to: the receiver
    - subject: the subject of the mail
    - files: an iterator of the file names as attachments

    :return: None

    """

    password = getpass.getpass('Enter your password: ')
    smtp_server = args['smtp_server']
    sender = args['sender']
    cc = ''
    smtp_port = args['smtp_port']
    to = args['to']
    subject = args['subject']
    files = args['attachments']
    body = args['body']

    # Pack email header
    msg = MIMEMultipart()
    msg['From'] = format_header('{}'.format(sender))
    msg['To'] = format_header('{}'.format(to))
    msg['Subject'] = subject

    if cc:
        msg['Bcc'] = format_header('Me <{}>'.format(cc))

    # Pack email contents and attachments.
    # All the files in the path will be attached in the email.
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    for file in files:
        with open(file, 'rb') as fp:
            mime = MIMEBase('application', 'octet-stream', filename=file.split('/')[-1])
            mime.add_header('Content-Disposition', 'attachment', filename=file.split('/')[-1])
            mime.add_header('Content-ID', '<0>')
            mime.add_header('X-Attachment-Id', '0')
            mime.set_payload(fp.read())
            encoders.encode_base64(mime)
            msg.attach(mime)

    # Sending the email
    if cc:
        to.append(cc)

    server = smtplib.SMTP(smtp_server, smtp_port)
    if args['verbose']:
        server.set_debuglevel(1)

    server.login(sender, password)
    server.sendmail(sender, to, msg.as_string())
    server.quit()

    return 0


def valid_email(s):
    if '@' in s:
        return s
    else:
        msg = "Not a valid email address: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def main():
    """The main function to parse arguments and invoke the send_mail function.
    """

    parser = argparse.ArgumentParser(description="The Utility for Sending Emails On a Server")

    parser.add_argument("sender", help="the sender, e.g, sender@example.com", type=valid_email)
    parser.add_argument("recipients", help="the recipients separated by semicolons", type=valid_email)

    parser.add_argument("-S", help="the smtp server", metavar='')
    parser.add_argument("-P", help="the smtp port", metavar='', default='25')
    parser.add_argument("-s", help="the subject of the email", metavar='')
    parser.add_argument("-a", help="the file(s) to be attached, separated by spaces", metavar='', nargs='*')

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-b", help="message body reading from a file or standard input: - ",
                        type=argparse.FileType('r'), metavar='')
    group.add_argument("-B", help="message body by a string", metavar='')

    parser.add_argument("-v", help="detailed print", action='store_true')

    args = parser.parse_args()

    files = []
    paths = args.a if args.a else []

    for attach_path in paths:
        if os.path.isdir(attach_path):
            for f in os.listdir(attach_path):
                full_path = os.path.join(attach_path, f)
                if os.path.isfile(full_path):
                    files.append(full_path)
        elif os.path.isfile(attach_path):
            files.append(attach_path)
        else:
            pass

    body = ''
    if args.B:
        body = args.B
    elif args.b:
        body += args.b.read()

    sender = args.sender

    if args.S:
        smtp_server = args.S
    else:
        smtp_server = 'smtp.{}'.format(sender.split('@')[1])

    smtp_port = args.P if args.P else '587'

    verbose = args.v
    params = {
        'sender': sender,
        'smtp_server': smtp_server,
        'smtp_port': smtp_port,
        'to': args.recipients,
        'subject': args.s,
        'body': body,
        'attachments': files,
        'verbose': verbose
    }

    if verbose:
        _print(params)

    send_mail(params)
    print('Sent.')
    return 0

if __name__ == '__main__':
    sys.exit(main())
