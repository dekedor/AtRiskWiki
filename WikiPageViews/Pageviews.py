import requests
from datetime import date
from dateutil.rrule import rrule, DAILY, MONTHLY
import re

def GetPageViewsTyped(title, start, end, user_type='all-agents'):
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
    url = 'http://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/%s/%s/daily/%d/%d' \
    % (user_type, title, start, end)
    data = requests.get(url).json()
    pageviews = {}
    for day in data['items']:
        pageviews[day['timestamp'][0:-2]] = day['views']
    return pageviews

def GetPageViews(title, start, end):
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
    pageviews_user = GetPageViewsTyped(title, start, end, 'user')
    pageviews_spider = GetPageViewsTyped(title, start, end, 'spider')
    pageviews_bot = GetPageViewsTyped(title, start, end, 'bot')
    pageviews = {}
    start_str = str(start)
    end_str = str(end)
    start_date = date(int(start_str[0:4]), int(start_str[4:6]), int(start_str[6:8]))
    end_date = date(int(end_str[0:4]), int(end_str[4:6]), int(end_str[6:8]))
    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        code = '%04d%02d%02d' % (dt.year, dt.month, dt.day)
        pageviews[code] = {}
        if code in pageviews_spider:
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
    return pageviews

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
            pageviews[field] = {}
            pageviews[field]['all'] = data['daily_views'][code]
    return pageviews
