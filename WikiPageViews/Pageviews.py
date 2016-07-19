import requests
from datetime import date
from dateutil.rrule import rrule, DAILY, MONTHLY
import re
import numpy as np
import pandas as pd
from os.path import isfile
from time import sleep

def GetPageViewsTyped(title, start, end, user_type='all-agents', s=None):
    """
    This function returns the page views by day for a given page going back to 
    October 1st, 2015 for a give user type. Please do not make more than 500 requests/s.

    inputs:
        title: page title string
        start: timestamp string for start date (yyyymmdd00, no idea what trailing 0's are for)
        end: timestamp string for start date (yyyymmdd00, no idea what trailing 0's are for)
        user_type: string indicating user type, options are 'all-agents', 'user', 'spider', 'bot'

    returns:
        pageviews: dictionary with timestamps as keys and number of views on that day as entries
    """
    # check if user ommitted superfluous trailing 0's
    if len(str(start)) < 10:
        start = start * 100
    if len(str(end)) < 10:
        end = end * 100
    # check if user wants inaccessible data
    if start < 2015100100:
        start = 2015100100
    if end < 2015100100:
        end = 2015100100
    if not s:
        s = requests.Session()
    url = 'http://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/%s/%s/daily/%d/%d' \
    % (user_type, title, start, end)
    data = s.get(url).json()
    pageviews = {}
    try:
        for day in data['items']:
            pageviews[day['timestamp'][0:-2]] = day['views']
    except:
        sleep(60)
        data = s.get(url).json()
        for day in data['items']:
            pageviews[day['timestamp'][0:-2]] = day['views']
    return pageviews, s

def GetPageViews(title, start, end, s=None):
    """
    Calls GetPageViewsTyped and returns data for individual user types.

    inputs:
        title: page title string
        start: timestamp string for start date (yyyymmdd00, no idea what trailing 0's are for)
        end: timestamp string for start date (yyyymmdd00, no idea what trailing 0's are for)

    returns:
        pageviews: dictionary with timestamps as keys and number of views on that day as entries
                   each user type has it's own entry for each day
    """
    if not s:
        s = requests.Session()
    try:
        pageviews_user, s = GetPageViewsTyped(title, start, end, 'user', s)
    except:
        sleep(60)
        pageviews_user, s = GetPageViewsTyped(title, start, end, 'user', s)
    try:
        pageviews_spider, s = GetPageViewsTyped(title, start, end, 'spider', s)
    except:
        sleep(60)
        pageviews_spider, s = GetPageViewsTyped(title, start, end, 'spider', s)
    try:
        pageviews_bot, s = GetPageViewsTyped(title, start, end, 'bot', s)
    except:
        sleep(60)
        pageviews_bot, s = GetPageViewsTyped(title, start, end, 'bot', s)
    pageviews = {}
    start_str = str(start)
    end_str = str(end)
    start_date = date(int(start_str[0:4]), int(start_str[4:6]), int(start_str[6:8]))
    end_date = date(int(end_str[0:4]), int(end_str[4:6]), int(end_str[6:8]))
    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        code = '%04d%02d%02d' % (dt.year, dt.month, dt.day)
        pageviews[code] = {}
        if code in pageviews_user:
            pageviews[code]['user'] = pageviews_user[code]
        else:
            pageviews[code]['user'] = 0
        if code in pageviews_spider:
            pageviews[code]['spider'] = pageviews_spider[code]
        else:
            pageviews[code]['spider'] = 0
        if code in pageviews_bot:
            pageviews[code]['bot'] = pageviews_bot[code]
        else:
            pageviews[code]['bot'] = 0
        pageviews[code]['all'] = pageviews[code]['user'] + pageviews[code]['spider'] + pageviews[code]['bot']
    return pageviews, s

def GetPageViewsPreOct2015(title, start, end, s=None):
    """
    This functions users http://stats.grok.se to get page view statistics.
    This is necessary to get data before October 2015.
    Note: sometime after October 2015 http://stats.grok.se stops tracking these statistics.
    Unlike for data after October 2015, only total views is stored on http://stats.grok.se.

    inputs:
        title: page title string
        start: start month (format yyyydd)
        end: end month (format yyyydd)

    output:
        pageviews: dictionary with timestamps as keys and number of views on that day as entries
                   each user type has it's own entry for each day        
    """
    start_str = str(start)
    end_str = str(end)
    start_date = date(int(start_str[0:4]), int(start_str[4:6]), 1)
    end_date = date(int(end_str[0:4]), int(end_str[4:6]), 1)
    pageviews = {}
    if not s:
        s = requests.Session()
    for dt in rrule(MONTHLY, dtstart=start_date, until=end_date):
        url = 'http://stats.grok.se/json/en/%04d%02d/%s' % (dt.year, dt.month, title)
        data = s.get(url).json()
        for code in data['daily_views']:
            field = re.sub('-', '', code)
            pageviews[field] = data['daily_views'][code]
    return pageviews, s

def DateString2Week(dateStr):
    try:
        return (date(int(dateStr[0:4]), int(dateStr[4:6]), int(dateStr[6:8])) - date(2001, 1, 14)).days//7
    except ValueError:
        return -1
    
def AggregateViewsByWeek(views, page_id):
    df = pd.DataFrame().from_dict(views, orient='index')
    df['Week'] = np.array([DateString2Week(item) for item in df.index])
    viewsByWeek = df.groupby('Week', sort=True).sum()
    viewsByWeek.columns = ['Views']
    viewsByWeek = viewsByWeek[viewsByWeek.index > 0]
    viewsByWeek['Page ID'] = np.array([page_id for week in viewsByWeek.index])
    viewsByWeek['Week'] = viewsByWeek.index
    viewsByWeek = viewsByWeek[['Page ID', 'Week', 'Views']]
    return viewsByWeek

def GrabAndWritePageViews(title, page_id, start, end, path, s):
    views, s = GetPageViewsPreOct2015(title, start, end, s)
    pageViews = AggregateViewsByWeek(views, page_id)
    header = not isfile(path)
    pageViews.to_csv(path, mode='a', header=header, index=False)
    return s
    
def GrabAndWritePageViewsPost2015(title, page_id, start, end, path, s=None):
    if not s:
        s = requests.Session()
    views, s = GetPageViews(title, start, end, s)
    pageViews = AggregateViewsByWeekPost2015(views, page_id)
    header = not isfile(path)
    pageViews.to_csv(path, mode='a', header=header, index=False)
    return s
    
def AggregateViewsByWeekPost2015(views, page_id):
    df = pd.DataFrame().from_dict(views, orient='index')
    df['Week'] = np.array([DateString2Week(item) for item in df.index])
    viewsByWeek = df.groupby('Week', sort=True).sum()
    viewsByWeek.columns = ['user_views', 'all_views', 'spider_views', 'bot_views']
    viewsByWeek = viewsByWeek[viewsByWeek.index > 0]
    viewsByWeek['page_id'] = np.array([page_id for week in viewsByWeek.index])
    viewsByWeek['Week'] = viewsByWeek.index
    viewsByWeek = viewsByWeek[['page_id', 'Week', 'all_views', 'user_views', 'spider_views', 'bot_views']]
    return viewsByWeek
