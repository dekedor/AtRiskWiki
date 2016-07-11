from pytrends.pyGTrends import pyGTrends # to get do pip install pytrends
import pandas as pd
import re
import os

def GetGoogleTrends(subject, deleteTempFile = False):
    """
    Grabs data from Google trends for a given topic.
    Creates a temporary csv file that can optionall be deleted after use.
    inputs
        subject: topic to search (string)
        deleteTempFile: if True, deltes temporary file created after use
    output
        Dataframe of number of searches for each week
    """
    google_username = "atriskwikipedia@gmail.com"
    google_password = "cdipsworkshop"
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
    # write temporary file then open in pandas... this is just easier, but might be a better way
    f = open(path, 'w+')
    f.write(data)
    f.close()
    df = pd.read_csv(path)
    if deleteTempFile:
        os.remove(path)
    return df
