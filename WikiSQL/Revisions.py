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
import pandas as pd
import re
from datetime import date, timedelta
from time import time
from os.path import isfile

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
        command = "SELECT revision.rev_timestamp, revision.rev_comment, user_groups.ug_group, page.page_id FROM revision \
           JOIN page on page.page_id = revision.rev_page AND page.page_title = '%s' \
           AND page.page_namespace = 0 LEFT JOIN user_groups ON revision.rev_user = user_groups.ug_user \
           ORDER BY revision.rev_timestamp DESC" % title
    else:
        command = "SELECT revision.rev_timestamp, revision.rev_comment, user_groups.ug_group, page.page_id FROM revision \
           JOIN page on page.page_id = revision.rev_page AND page.page_id = '%s' \
           AND page.page_namespace = 0 LEFT JOIN user_groups ON revision.rev_user = user_groups.ug_user \
           ORDER BY revision.rev_timestamp DESC" % int(title)
    cur.execute(command)
    data = np.array(cur.fetchall())
    if data.size == 0:
        return None
    last_stamp = datetime.datetime(1, 1, 1, 0, 0, 0)
    last_comment = 'aaaaaaaaaaaaa'
    pageID = np.array([int(item) for item in data[:,3]])
    timestamp = np.array([Timestamp2Datetime(int(item)) for item in data[:,0]])
    isAdmin = np.array([item in [b'admin', b'sysop'] for item in data[:, 2]])
    isConfirmed = np.array([item in [b'admin', b'sysop', b'confirmed', b'autoconfirmed', b'extendedconfirmed'] \
                            for item in data[:, 2]])
    isRevert = np.array([('revert' in item.decode('utf-8', 'ignore').lower()) or \
                         ('rv ' in item.decode('utf-8', 'ignore').lower()) \
                         or 'undid' in item.decode('utf-8', 'ignore').lower() if item else False for item in data[:, 1]])
    mentionsVandal = np.array([('vandal' in item.decode('utf-8', 'ignore').lower()) if item else False \
                   for item in data[:, 1]])
    comments = np.array([item.decode('utf-8', 'ignore') if item else None for item in data[:, 1]])
    protected = np.array([('Protected' in item.decode('utf-8', 'ignore')) or  \
                          ('protection level' in item.decode('utf-8', 'ignore')) or \
                          ('prot req' in item.decode('utf-8', 'ignore')) or \
                          ('Tagging page' in item.decode('utf-8', 'ignore'))
                          if item else False for item in data[:, 1]])
    unprotected = np.array([('Unprotected' in item.decode('utf-8', 'ignore')) or \
                            ('not protected' in item.decode('utf-8', 'ignore')) or \
                            ('Removing' in item.decode('utf-8', 'ignore'))
                            if item else False for item in data[:, 1]])
    edit_priv = []
    move_priv = []
    for item in data[:, 1]:
        if item:
            if 'edit=' in item.decode('utf-8', 'ignore'):
                temp = re.findall('edit=[a-z]+', item.decode('utf-8', 'ignore'))
                if len(temp) > 0:
                    edit_priv.append(temp[0])
                else:
                    edit_priv.append(None)
            else:
                edit_priv.append(None)
            if 'move=' in item.decode('utf-8', 'ignore'):
                temp = re.findall('move=[a-z]+', item.decode('utf-8', 'ignore'))
                if len(temp) > 0:
                    move_priv.append(temp[0])
                else:
                    move_priv.append(None)
            else:
                move_priv.append(None)
        else:
            edit_priv.append(None)
            move_priv.append(None)
    pageIDNoDup = []
    timestampNoDup = []
    isAdminNoDup = []
    isConfirmedNoDup = []
    isRevertNoDup = []
    mentionsVandalNoDup = []
    commentsNoDup = []
    protectedNoDup = []
    unprotectedNoDup = []
    edit_privNoDup = []
    move_privNoDup = []
    for pid, stamp, admin, confirmed, revert, vandal, rev_com, protect, unprotect, edit, move \
    in zip(pageID, timestamp, isAdmin, isConfirmed, isRevert, mentionsVandal, comments, protected, unprotected, \
           edit_priv, move_priv):
        if stamp == last_stamp and \
        rev_com == last_comment:
            if admin:
                isAdminNoDup[-1] = admin
            if confirmed:
                isConfirmedNoDup[-1] = confirmed
        else:
            last_stamp = stamp
            pageIDNoDup.append(pid)
            timestampNoDup.append(stamp)
            isAdminNoDup.append(admin)
            isConfirmedNoDup.append(confirmed)
            isRevertNoDup.append(revert)
            mentionsVandalNoDup.append(vandal)
            commentsNoDup.append(rev_com)
            protectedNoDup.append(protect)
            unprotectedNoDup.append(unprotect)
            edit_privNoDup.append(edit)
            move_privNoDup.append(move)
    series = {}
    series['Page ID'] = pd.Series(data=pageIDNoDup)
    series['Timestamp'] = pd.Series(data=timestampNoDup)
    series['AdminUser'] = pd.Series(data=isAdminNoDup)
    series['ConfirmedUser'] = pd.Series(data=isConfirmedNoDup)
    series['Revert'] = pd.Series(data=isRevertNoDup)
    series['MentionsVandalism'] = pd.Series(data=mentionsVandalNoDup)
    series['Comment'] = pd.Series(data=commentsNoDup)
    series['Protected'] = pd.Series(data=protectedNoDup)
    series['Unprotected'] = pd.Series(data=unprotectedNoDup)
    series['Edit Priv'] = pd.Series(data=edit_privNoDup)
    series['Move Priv'] = pd.Series(data=move_privNoDup)
    df = pd.DataFrame(series)
    df = df[['Page ID', 'Timestamp', 'AdminUser', 'ConfirmedUser', 'Revert', 'MentionsVandalism', 'Protected', 'Unprotected', \
            'Edit Priv', 'Move Priv', 'Comment']]
    return df

def RevisionToWeeklyAgg(revs):
    series = {}
    revs['Date'] = np.array([date.fromtimestamp(item.astype('O')/1e9) for item in revs['Timestamp'].values])
    revs['Week'] = np.array([(item - date(2001, 1, 14)).days//7 for item in revs['Date'].values])
    vandals = revs[revs['MentionsVandalism'] == True].groupby('Week', sort=True).count()
    df_vandals = pd.DataFrame(data=vandals['MentionsVandalism'].values, index=vandals.index, columns=['MentionsVandalism'])
    df_vandals['Week'] = df_vandals.index
    reverts = revs[revs['Revert'] == True].groupby('Week', sort=True).count()
    df_reverts = pd.DataFrame(data=reverts['Revert'].values, index=reverts.index, columns=['Revert'])
    df_reverts['Week'] = df_reverts.index
    revisions = revs.groupby('Week', sort=True).count()
    df_revisions = pd.DataFrame(data=revisions['MentionsVandalism'].values, index=revisions.index, columns=['Revisions'])
    df_revisions['Week'] = df_revisions.index
    df = pd.merge(df_revisions, df_vandals, on='Week', how='outer')
    df = pd.merge(df, df_reverts, on='Week', how='outer')
    df['Page ID'] = np.array([revs['Page ID'].values[0] for item in df['Week']])
    df = df[['Page ID', 'Week', 'Revisions', 'Revert', 'MentionsVandalism']]
    df = df.fillna(0)
    df = df.sort_values(by='Week')
    revs_cut = revs[(revs['Protected'] == True) | (revs['Unprotected'] == True)]
    return df, revs_cut

def WriteRevsToFile(df, start, end, path1, path2):
    revs = [GetRevisions(df['page_id'].values[i]) for i in range(start, end)]
    end = end - start
    start = 0
    dfs = [RevisionToWeeklyAgg(revs[i]) if revs[i] is not None else None for i in range(start, end)]
    for item in dfs:
        if item is not None:
            header = not isfile(path1)
            item[0].to_csv(path1, mode='a', header=header, index=False)
            item[1].to_csv(path2, mode='a', header=header, index=False)
