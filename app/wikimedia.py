#!/usr/bin/env python
"""
Retrieve page visits using Wikimedia API
More info on: https://www.kaggle.com/c/web-traffic-time-series-forecasting/discussion/36481
"""

import datetime
import json
import numpy as np
import os
import pandas as pd
import progressbar
import requests
import urllib

from dao import SimpleDao
from csv import QUOTE_NONNUMERIC

API_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "{project}/{access}/{agent}/{name}/daily/{from_date}/{to_date}"
)


def get_views(project, access, agent, name, from_date, to_date, flatten=True):
    from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d").strftime("%Y%m%d00")
    to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d").strftime("%Y%m%d00")
    url = API_URL.format(
        project=project, access=access, agent=agent, name=urllib.quote_plus(name),
        from_date=from_date, to_date=to_date
    )
    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError("API call failed")
    data = json.loads(response.content)[u'items']
    dates = [datetime.datetime.strptime(daily_data[u'timestamp'], '%Y%m%d00').date() for daily_data in data]
    views = [daily_data[u'views'] for daily_data in data]
    if not flatten:
        return pd.Series(views, index=[date.strftime('%Y-%m-%d') for date in dates])
    return pd.DataFrame(np.column_stack([dates, views]), columns=['date', 'visits'])


def collect_views():
    dao = SimpleDao('localhost', '5432', 'postgres', 'postgres', 'kaggle-wttsf')
    dao.run_query('DROP TABLE IF EXISTS train_flat_split_2')
    dao.run_query(
        'CREATE TABLE train_flat_split_2 '
        '(page TEXT, project TEXT, access TEXT, agent TEXT, "name" TEXT, dow FLOAT8, '
        '"month" FLOAT8, "date" TIMESTAMP, visits FLOAT8)'
    )
    pages = dao.download_from_query('SELECT DISTINCT page FROM train_flat_split')
    for index, row in pages.iterrows():
        name, project, access, agent = row['page'].rsplit('_', 3)
        views = get_views(project, access, agent, name.encode('utf-8'), '2017-01-01', '2017-09-01')
        views['page'] = row['page']
        views['project'] = project
        views['access'] = access
        views['agent'] = agent
        views['name'] = name
        views['date'] = pd.to_datetime(views['date'])
        views['dow'] = views['date'].dt.dayofweek
        views['month'] = views['date'].dt.month
        dao.upload_from_dataframe(views, 'train_flat_split_2', if_exists='append')


def build_train(min_date='2017-01-01', max_date='2017-09-01'):
    # Get all pages from train_1.csv
    pages = pd.read_csv(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'input', 'train_1.csv'),
        delimiter=',', quotechar='"', usecols=['Page'], encoding='utf-8'
    )
    # Build new train dataset
    train = pd.DataFrame()
    bar = progressbar.ProgressBar()
    for page in bar(pages['Page'].tolist()):
        name, project, access, agent = page.rsplit('_', 3)
        while True:
            try:
                views = get_views(project, access, agent, name.encode('utf-8'), min_date, max_date, flatten=False)
                break
            except RuntimeError:
                print 'Error: {}'.format(page)
        views['Page'] = page
        views['project'] = project
        views['access'] = access
        views['agent'] = agent
        views['name'] = name
        train = train.append(views, ignore_index=True)
    # Get list of dates
    min_date = datetime.datetime.strptime(min_date, '%Y-%m-%d')
    max_date = datetime.datetime.strptime(max_date, '%Y-%m-%d')
    dates = [(min_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in xrange((max_date - min_date).days)]
    # Save DataFrame to a csv file
    train[dates] = train[dates].astype(int)
    train.to_csv(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'input', 'train_2_custom.csv'),
        columns=['Page', 'project', 'access', 'agent', 'name'] + dates, index=False,
        encoding='utf-8', quoting=QUOTE_NONNUMERIC, quotechar='"'
    )


if __name__ == '__main__':
    # print get_views('zh.wikipedia.org', 'all-access', 'spider', '2NE1', '2017-01-01', '2017-09-01')
    # collect_views()
    build_train()
