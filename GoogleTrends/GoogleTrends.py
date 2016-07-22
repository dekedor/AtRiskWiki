from pytrends.pyGTrends import pyGTrends # to get do pip install pytrends
import pandas as pd
import re
import os

def GetGoogleTrends(subject, page_id, deleteTempFile = False):
    """
    Grabs data from Google trends for a given topic.
    Creates a temporary csv file that can optionall be deleted after use.
    inputs
        subject: topic to search (string)
        deleteTempFile: if True, deltes temporary file created after use
    output
        Dataframe of number of searches for each week
    """
    subject = re.sub(',', '', subject)
    google_username = "the.kevin.osullivan@gmail.com"
    google_password = "d4rks1v3r"
    path = '%s_temp.csv' % subject
    connector = pyGTrends(google_username, google_password)
    connector.request_report(subject)
    data = connector.decode_data
    # so... data is a string intended to become a csv file
    # the first four lines are useless and need to be removed
    # there is likely a better way to do this than what I do below, but it works
    data = re.sub('Web.*', '', data)
    data = re.sub('World.*', '', data)
    data = re.sub('World.*', '', data)
    data = re.sub('Interest.*', '', data)
    data = re.sub('\n\n\n\n', '', data)
    data = re.split('.*, \n', data)[0]
    data = re.split('\n\n', data)[0]
    # write temporary file then open in pandas... this is just easier, but might be a better way
    f = open(path, 'w+')
    f.write(data)
    f.close()
    try:
        df = pd.read_csv(path)
    except:
        return None
    df.columns = ['Week', 'Searches']
    df['Week'] = [re.sub('-', '', re.split(' - ', week)[0]) for week in df['Week']]
    df['page_id'] = [page_id for week in df['Week']]
    df = df[['page_id', 'Week', 'Searches']]
    if deleteTempFile:
        os.remove(path)
    return df

def DressTitle(title):
    s = re.sub('\(', '', title)
    s = re.sub('\)', '', s)
    s = re.sub('_', ' ', s)
    return s
