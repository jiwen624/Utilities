#!/usr/bin/env python3
from email import encoders
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
from email.mime.base import MIMEBase
import getpass
import smtplib

def format_header(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))

#Parameters:
#   - to: the receiver
#   - subject: the subject of the mail
#   - fileiter: an iterator of the file names as attachments

def sendmail(parms):
    #Hard-coded parameters
    password = getpass.getpass()
    smtp_server = 'delivery.mygridway.net' #TODO
    from_addr = 'eyang@diablo-technologies.com'
    cc = 'eyang@diablo-technologies.com'
    smtp_port = 587

    from_str = parms['from']
    to = parms['to']
    subject = parms['subject']
    fileiter = parms['fileiter']
    text = parms['text']

    #Pack email header
    msg = MIMEMultipart()
    msg['From'] = format_header('%s <%s>' % (from_str, from_addr))
    msg['To'] = format_header('Receiver <%s>' % to)
    msg['Bcc'] = format_header('Me <%s>' % cc)
    msg['Subject'] = subject

    #Pack email contents and attachments. All the files in the path will be attached in the email.
    msg.attach(MIMEText(text, 'plain', 'utf-8'))

    for file in fileiter:
        with open(file, 'rb') as fp:
            mime = MIMEBase('application', 'octet-stream', filename=file.split('/')[-1])
            mime.add_header('Content-Disposition', 'attachment', filename=file.split('/')[-1])
            mime.add_header('Content-ID', '<0>')
            mime.add_header('X-Attachment-Id', '0')
            mime.set_payload(fp.read())
            encoders.encode_base64(mime)
            msg.attach(mime)


    #Sending the email
    to = [to, cc] 
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.set_debuglevel(1)
    server.login(from_addr, password)
    server.sendmail(from_addr, to, msg.as_string())
    server.quit()


if __name__ == '__main__':
    #Read input parameters and assert
    import os
    import sys

    if len(sys.argv) != 4:
        print('Usage: mailto [path_to_attach] [mail_recipients] [mail_subject]')
        exit(1)

    dir = sys.argv[1]
    to = sys.argv[2]
    subject = sys.argv[3] 
    
    if os.path.isdir(dir):
        files = (os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f)))
    elif os.path.isfile(dir):
        files = [dir] 
    else:
        print('Error: not a valid path.')
        exit(1)    

    parms = {
        'from': 'Eric Yang',
        #'to': 'mamer@diablo-technologies.com',
        'to': to,
        'subject': subject,
        'text': 'Please see attached files.',
        'fileiter': files
    }
    sendmail(parms)
    exit(0)
