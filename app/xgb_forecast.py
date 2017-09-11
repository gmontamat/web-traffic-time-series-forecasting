#!/usr/bin/env python
"""
Forecast time series using Xgboost models
"""

import datetime
import numpy as np

from dao import SimpleDao
from xgb import XgboostRegressor


TODAY = datetime.date.today().strftime('%Y-%m-%d')
RESPONSE = 'visits'
INDEX_COLUMNS = ['page', 'date']
COLUMNS = [
    'page',
    # 'name',
    # 'project',
    'access',
    # 'agent',
    'dow',
    'month',
    'date',
]


def load_train(project, agent, min_lag, max_lag):
    # Get table with features and response variable
    dao = SimpleDao('localhost', '5432', 'postgres', 'postgres', 'kaggle-wttsf')
    columns = ', '.join(COLUMNS + ['visits_lag{}'.format(i) for i in xrange(min_lag, max_lag+1)] + [RESPONSE])
    train = dao.download_from_query(
        "SELECT {} FROM xy2 WHERE date>='2016-09-01' AND project='{}' AND agent={}".format(
            columns, project, agent
        )
    )
    train.fillna(0.0, inplace=True)
    return train


def generate_baseline(train):
    """Generate an initial estimate for the Xgboost model
    """
    history = [feature for feature in list(train) if 'visits_lag' in feature]
    return train[history].median(axis=1)


def train_model(features, response, baseline=None, model_name='xgboost_{}'.format(TODAY)):
    if baseline is None:
        baseline = np.median(response)
    # Set model parameters
    params = {
        'objective': 'reg:linear',
        'eval_metric': 'mae',
        'n_trees': 3000,
        'eta': 0.01,
        'max_depth': 9,
        'subsample': 0.90,
        'base_score': baseline,
        'silent': 1
    }
    xgb = XgboostRegressor(params=params, name=model_name)
    xgb.train_model(features.as_matrix(), response.as_matrix())
    # Save model
    xgb.save_model('../models')


def load_test(date, project, agent, min_lag, max_lag):
    # Get table with test features
    dao = SimpleDao('localhost', '5432', 'postgres', 'postgres', 'kaggle-wttsf')
    columns = ', '.join(COLUMNS + ['visits_lag{}'.format(i) for i in xrange(min_lag, max_lag + 1)])
    test = dao.download_from_query(
        "SELECT {} FROM testx2 WHERE date='{}' AND project='{}' AND agent={}".format(
            columns, date, project, agent
        )
    )
    return test


def predict(test, clip_negative=True, model_name='xgboost_{}'.format(TODAY)):
    # Load model
    xgb = XgboostRegressor(model_path='../models', name=model_name)
    # Predict
    forecast = xgb.predict(test.as_matrix())
    if clip_negative:
        forecast = forecast.clip(min=0.0)
    return forecast


def main():
    lag_min = 14
    lag_days = 7
    min_date = datetime.datetime.strptime('2017-09-13', '%Y-%m-%d')
    max_date = datetime.datetime.strptime('2017-11-13', '%Y-%m-%d')
    dates = [
        (min_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in xrange((max_date - min_date).days + 1)
    ]
    projects = [
        'en.wikipedia.org',
        'ja.wikipedia.org',
        'de.wikipedia.org',
        'fr.wikipedia.org',
        'zh.wikipedia.org',
        'ru.wikipedia.org',
        'es.wikipedia.org',
        'commons.wikimedia.org',
        'www.mediawiki.org'
    ]
    agents = [0, 1]
    for project in projects:
        for agent in agents:
            for i, date in enumerate(dates):
                name = 'lag{}_{}_{}'.format(lag_min+i, project, agent)
                # Train model on available data
                train = load_train(project, agent, lag_min+i, lag_min+i+lag_days)
                baseline = generate_baseline(train)
                train_model(train.drop(INDEX_COLUMNS + [RESPONSE], axis=1), train[RESPONSE], baseline, model_name=name)
                train = None  # Give the garbage collector a hand
                # Predict using trained model
                test = load_test(date, project, agent, lag_min+i, lag_min+i+lag_days)
                test['visits'] = predict(test.drop(INDEX_COLUMNS, axis=1), model_name=name)
                test.to_csv(
                    '../output/{}.csv'.format(name), columns=['page', 'date', 'visits'], index=False, encoding='utf-8'
                )
                test = None  # Give the garbage collector a hand


if __name__ == '__main__':
    main()
