"""
This only works if you've already used ssh tunnerl into Tool Labs. The command is:
ssh -L 4711:enwiki.labsdb:3306 username@tools-login.wmflabs.org
To get a user account at Tool Labs, follow the instrunctions here:
https://wikitech.wikimedia.org/wiki/Help:Tool_Labs/Access
You will need admin approval which takes ~1 day.
After you ssh into there server there will be a file called replica.my.cnf. Your username and password below will be in that file.
"""

import numpy as np
import pymysql # to get, do pip install PyMySQL
import datetime

def Timestamp2Datetime(timestamp):
    """
    Converts timestamps from the Tools DB into datetime objects.

    input:
        timestamp: timestamp from Tools DB (format yyyymmddhhmmss)

    output:
        datetime object
    """
    timestamp_str = str(timestamp)
    year = int(timestamp_str[0:4])
    month = int(timestamp_str[4:6])
    day = int(timestamp_str[6:8])
    hour = int(timestamp_str[8:10])
    minute = int(timestamp_str[10:12])
    second = int(timestamp_str[12:14])
    return datetime.datetime(year, month, day, hour, minute, second)

def GetRevisionTimeStamps(title):
    """
    Querries Tools DB and returns the date and times of all revisions to a particular
    article. Can accept article title or page id.

    This function is WAY faster if given a page id.

    input:
        title: page title string or page id

    output:
        stamps: numpy array of datetimes when revisions occured
    """
    username = 'u15045' # get from replica.my.cnf
    password = 'vlRyT64EYIOsSanN' # get from replica.my.cnf
    db=pymysql.connect(host='localhost',port=4711,user=username,passwd=password,db='enwiki_p')
    cur = db.cursor()
    if type(title) == str:
        command = "SELECT revision.rev_timestamp FROM revision JOIN page on page.page_id = revision.rev_page \
        WHERE page.page_title = '%s' ORDER BY revision.rev_timestamp DESC" % title
    elif type(title) == int:
        command = "SELECT revision.rev_timestamp FROM revision \
        WHERE revision.rev_page = '%d' ORDER BY revision.rev_timestamp DESC" % title
    cur.execute(command)
    data = np.array(cur.fetchall())
    stamps = np.array([Timestamp2Datetime(int(item[0])) for item in data])
    return stamps

def GetPageCreationDate(title):
    """
    Get the creation date of an article by looking for first revision.

    input:
        title: page title string

    output:
        stamp: datetime of page creation
    """
    username = 'u15045' # get from replica.my.cnf
    password = 'vlRyT64EYIOsSanN' # get from replica.my.cnf
    db=pymysql.connect(host='localhost',port=4711,user=username,passwd=password,db='enwiki_p')
    cur = db.cursor()
    command = "SELECT revision.rev_timestamp FROM revision JOIN page on page.page_id = revision.rev_page \
    WHERE page.page_title = '%s' ORDER BY revision.rev_timestamp DESC LIMIT 1" % title
    cur.execute(command)
    data = cur.fetchall()
    stamp = Timestamp2Datetime(int(data[0][0]))
    return stamp
