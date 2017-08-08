#!/usr/bin/env python

"""
Retrieve page visits using Wikimedia API
More info on: https://www.kaggle.com/c/web-traffic-time-series-forecasting/discussion/36481
"""

import datetime
import json
import numpy as np
import pandas as pd
import requests


API_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "{project}/{access}/{agent}/{name}/daily/{from_date}/{to_date}"
)


def get_views(project, access, agent, name, from_date, to_date):
    from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d").strftime("%Y%m%d00")
    to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d").strftime("%Y%m%d00")
    url = API_URL.format(project=project, access=access, agent=agent, name=name, from_date=from_date, to_date=to_date)
    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError("API call failed")
    data = json.loads(response.content)[u'items']
    dates = [datetime.datetime.strptime(daily_data[u'timestamp'], '%Y%m%d00').date() for daily_data in data]
    views = [daily_data[u'views'] for daily_data in data]
    return pd.DataFrame(np.column_stack([dates, views]), columns=['date', 'views'])


if __name__ == '__main__':
    print get_views('zh.wikipedia.org', 'all-access', 'spider', '2NE1', '2017-01-01', '2017-07-31')
