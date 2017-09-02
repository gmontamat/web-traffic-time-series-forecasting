#!/usr/bin/env python

"""
Use median as prediction
Source: https://www.kaggle.com/clustifier/weekend-weekdays/code
"""

import datetime
import os
import numpy as np
import pandas as pd

from load_data import load_train, load_flat_train


def replace_outliers(df, replace_with=np.nan):
    cols = list(df)[1:]
    df = df.assign(q10=df.quantile(0.1, axis=1))
    df = df.assign(q90=df.quantile(0.9, axis=1))
    df = df.assign(upper=df['q90'] + 2.0 * (df['q90'] - df['q10']))
    df = df.assign(lower=df['q10'] - 2.0 * (df['q90'] - df['q10']))
    for col in cols:
        df.loc[df[col] < df['lower'], col] = replace_with
        df.loc[df[col] > df['upper'], col] = replace_with
    df.drop(['q10', 'q90', 'lower', 'upper'], axis=1, inplace=True)
    return df


def median_week():
    train = load_train()
    train_flattened = pd.melt(
        train[list(train.columns[-49:]) + ['Page']], id_vars='Page', var_name='date', value_name='Visits'
    )
    train_flattened['date'] = train_flattened['date'].astype('datetime64[ns]')
    train_flattened['weekend'] = (train_flattened.date.dt.dayofweek >= 5).astype(float)

    test = pd.read_csv(os.path.join('..', 'input', 'key_1.csv'))
    test['date'] = test.Page.apply(lambda a: a[-10:])
    test['Page'] = test.Page.apply(lambda a: a[:-11])
    test['date'] = test['date'].astype('datetime64[ns]')
    test['weekend'] = (test.date.dt.dayofweek >= 5).astype(float)

    train_page_per_dow = train_flattened.groupby(['Page', 'weekend']).median().reset_index()

    test = test.merge(train_page_per_dow, how='left')
    test.loc[test.Visits.isnull(), 'Visits'] = 0

    test[['Id', 'Visits']].to_csv(os.path.join('..', 'output', 'submission_median.csv'), index=False)


def median_forecast(remove_outliers=False, date_min=None, date_max=None):
    train = load_train()
    if remove_outliers:
        train = replace_outliers(train)
    train_flattened = pd.melt(
        train[list(train.columns[-49:]) + ['Page']], id_vars='Page', var_name='date', value_name='Visits'
    )
    train_flattened['date'] = train_flattened['date'].astype('datetime64[ns]')
    if date_min and date_max:
        date_min = datetime.datetime.strptime(date_min, '%Y-%m-%d')
        date_max = datetime.datetime.strptime(date_max, '%Y-%m-%d')
        train_flattened = train_flattened[(train_flattened['date'] >= date_min) & (train_flattened['date'] <= date_max)]
    train_flattened['dayofweek'] = (train_flattened.date.dt.dayofweek >= 5).astype(float)

    test = pd.read_csv(os.path.join('..', 'input', 'key_1.csv'))
    test['date'] = test.Page.apply(lambda a: a[-10:])
    test['Page'] = test.Page.apply(lambda a: a[:-11])
    test['date'] = test['date'].astype('datetime64[ns]')
    test['dayofweek'] = (test.date.dt.dayofweek >= 5).astype(float)

    train_page_per_dow = train_flattened.groupby(['Page', 'dayofweek']).median().reset_index()

    test = test.merge(train_page_per_dow, how='left')
    test.loc[test.Visits.isnull(), 'Visits'] = 0

    test[['Id', 'Visits']].to_csv(os.path.join('..', 'output', 'submission_median.csv'), index=False)


def naive_last_year():
    train_flat = load_flat_train()
    # Filter last year values
    date_min = datetime.datetime.strptime('2016-01-01', '%Y-%m-%d')
    date_max = datetime.datetime.strptime('2016-03-01', '%Y-%m-%d')
    date_remove = datetime.datetime.strptime('2016-02-29', '%Y-%m-%d')
    train_flat = train_flat[
        (train_flat['Date'] >= date_min) & (train_flat['Date'] <= date_max) & (train_flat['Date'] != date_remove)
    ]
    # Change dates
    train_flat['Date'] = train_flat['Date'].apply(lambda x: x + pd.DateOffset(years=1))
    # Save submission
    test = pd.read_csv(os.path.join('..', 'input', 'key_1.csv'))
    test['Date'] = test.Page.apply(lambda a: a[-10:])
    test['Page'] = test.Page.apply(lambda a: a[:-11])
    test['Date'] = test['Date'].astype('datetime64[ns]')
    test = test.merge(train_flat, how='left')
    test.loc[test.Visits.isnull(), 'Visits'] = 0
    test[['Id', 'Visits']].to_csv(os.path.join('..', 'output', 'submission_naively.csv'), index=False)


if __name__ == '__main__':
    # median_week()
    median_forecast(remove_outliers=True)
    # naive_last_year()
