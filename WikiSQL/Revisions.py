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
        # page.page_namespace = 0 ensures we are grabbing a main article
        command = "SELECT revision.rev_timestamp FROM revision JOIN page on page.page_id = revision.rev_page \
        AND page.page_title = '%s' AND page.page_namespace = 0 ORDER BY revision.rev_timestamp DESC" % title
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
    # page.page_namespace = 0 ensures we are grabbing a main article
    command = "SELECT revision.rev_timestamp FROM revision JOIN page on page.page_id = revision.rev_page \
    WHERE page.page_title = '%s' AND page.page_namespace = 0 ORDER BY revision.rev_timestamp DESC LIMIT 1" % title
    cur.execute(command)
    data = cur.fetchall()
    stamp = Timestamp2Datetime(int(data[0][0]))
    return stamp

def GetRevisions(title):
    """
    Querries Tools DB and returns the datetimes and whether edit was made by admin or confirmed us of all revisions 
    to a particular article. Also looks at comments for mentions of reversion or vandalism.
    Can accept article title or page id.

    input:
        title: page title string or page id

    output:
        df: pandas dataframe with three columns
            -AdminUser: is this user an admin?
            -ConfirmedUSer: is this user confirmed? (all admins are considered confirmed)
            -MentionsVandalism: do the comments mention vandalism?
            -Revert: do the comments say this is a reversion?
            -Timestamp: datetime of revision
    """
    username = 'u15045' # get from replica.my.cnf
    password = 'vlRyT64EYIOsSanN' # get from replica.my.cnf
    db=pymysql.connect(host='localhost',port=4711,user=username,passwd=password,db='enwiki_p')
    cur = db.cursor()
    if type(title) == str:
        # page.page_namespace = 0 ensures we are grabbing a main article
        command = "SELECT revision.rev_timestamp, revision.rev_comment, user_groups.ug_group FROM revision \
           JOIN page on page.page_id = revision.rev_page AND page.page_title = '%s' \
           AND page.page_namespace = 0  JOIN user_groups ON revision.rev_user = user_groups.ug_user \
           ORDER BY revision.rev_timestamp DESC" % title
    elif type(title) == int:
        command = "SELECT revision.rev_timestamp, revision.rev_comment, user_groups.ug_group FROM revision \
           JOIN page on page.page_id = revision.rev_page AND page.page_id = '%s' \
           AND page.page_namespace = 0  JOIN user_groups ON revision.rev_user = user_groups.ug_user \
           ORDER BY revision.rev_timestamp DESC" % title
    cur.execute(command)
    data = np.array(cur.fetchall())
    last_stamp = datetime.datetime(1, 1, 1, 0, 0, 0)
    timestamp = np.array([Timestamp2Datetime(int(item)) for item in data[:,0]])
    isAdmin = np.array([item in [b'admin', b'sysop'] for item in data[:, 2]])
    isConfirmed = np.array([item in [b'admin', b'sysop', b'confirmed', b'autoconfirmed', b'extendedconfirmed'] \
                            for item in data[:, 2]])
    isRevert = np.array([('revert' in item.decode('utf-8').lower()) or ('rv ' in item.decode('utf-8').lower()) \
                   for item in data[:, 1]])
    mentionsVandal = np.array([('vandal' in item.decode('utf-8').lower()) \
                   for item in data[:, 1]])
    timestampNoDup = []
    isAdminNoDup = []
    isConfirmedNoDup = []
    isRevertNoDup = []
    mentionsVandalNoDup = []
    for stamp, admin, confirmed, revert, vandal in zip(timestamp, isAdmin, isConfirmed, isRevert, mentionsVandal):
        if stamp == last_stamp:
            if admin:
                isAdminNoDup[-1] = admin
            if confirmed:
                isConfirmedNoDup[-1] = confirmed
        else:
            last_stamp = stamp
            timestampNoDup.append(stamp)
            isAdminNoDup.append(admin)
            isConfirmedNoDup.append(confirmed)
            isRevertNoDup.append(revert)
            mentionsVandalNoDup.append(vandal)
    series = {}
    series['Timestamp'] = pd.Series(data=timestampNoDup)
    series['AdminUser'] = pd.Series(data=isAdminNoDup)
    series['ConfirmedUser'] = pd.Series(data=isConfirmedNoDup)
    series['Revert'] = pd.Series(data=isRevertNoDup)
    series['MentionsVandalism'] = pd.Series(data=mentionsVandalNoDup)
    df = pd.DataFrame(series)
    return df
