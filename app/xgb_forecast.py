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
        "SELECT {} FROM xy WHERE date >= '2016-01-01' AND date < '2017-01-01' AND project='{}' AND agent={}".format(
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


def load_test(project, agent, min_lag, max_lag):
    # Get table with test features
    dao = SimpleDao('localhost', '5432', 'postgres', 'postgres', 'kaggle-wttsf')
    columns = ', '.join(COLUMNS + ['visits_lag{}'.format(i) for i in xrange(min_lag, max_lag + 1)])
    test = dao.download_from_query(
        "SELECT {} FROM testx WHERE date >= '2017-01-01' AND date <= '2017-03-01' AND project='{}' AND agent={}".format(
            columns, project, agent
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


def save_results(test, model_name='{}'.format(TODAY)):
    dao = SimpleDao('localhost', '5432', 'postgres', 'postgres', 'kaggle-wttsf', 'public')
    dao.upload_from_dataframe(test[INDEX_COLUMNS + [RESPONSE]], 'forecast_{}'.format(model_name))


def main():
    # Lag 12
    name = 'lag12_en.wikipedia.org_all-0'
    train = load_train('en.wikipedia.org', 0, 12, 12+1*7)
    baseline = generate_baseline(train)
    train_model(train.drop(INDEX_COLUMNS + [RESPONSE], axis=1), train[RESPONSE], baseline, model_name=name)


if __name__ == '__main__':
    main()
