#!/usr/bin/env python
"""
Code to generate submission out of multiple output files
"""

import datetime
import numpy as np
import os
import pandas as pd


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


def median_forecast(remove_outliers=False):
    # Get list of days
    min_date = datetime.datetime.strptime('2016-07-01', '%Y-%m-%d')
    max_date = datetime.datetime.strptime('2017-08-31', '%Y-%m-%d')
    dates = [
        (min_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in xrange((max_date - min_date).days + 1)
    ]
    train = pd.read_csv('../input/train_2.csv', delimiter=',', quotechar='"', usecols=["Page"] + dates)
    if remove_outliers:
        train = replace_outliers(train)
    train_flattened = pd.melt(
        train[list(train.columns[-49:]) + ['Page']], id_vars='Page', var_name='date', value_name='baseline'
    )
    train_flattened['date'] = train_flattened['date'].astype('datetime64[ns]')
    train_flattened['dayofweek'] = (train_flattened.date.dt.dayofweek >= 5).astype(float)

    test = pd.read_csv(os.path.join('..', 'input', 'key_2.csv'))
    test['date'] = test.Page.apply(lambda a: a[-10:])
    test['Page'] = test.Page.apply(lambda a: a[:-11])
    test['date'] = test['date'].astype('datetime64[ns]')
    test['dayofweek'] = (test.date.dt.dayofweek >= 5).astype(float)

    train_page_per_dow = train_flattened.groupby(['Page', 'dayofweek']).median().reset_index()

    test = test.merge(train_page_per_dow, how='left')
    test.loc[test.baseline.isnull(), 'baseline'] = 0

    return test[['Id', 'baseline']]


def generate_submission():
    # Load xgboost submission files
    df_list = []
    for root, dirs, files in os.walk(os.path.join('..', 'output')):
        for name in files:
            if 'lag' in name and name.endswith('.csv'):
                df_list.append(pd.read_csv(os.path.join(root, name), delimiter=',', quotechar='"', encoding='utf-8'))
    results = pd.concat(df_list)
    results['Page'] = results['page'] + '_' + results['date']
    results.drop(['page', 'date'], axis=1, inplace=True)
    results.rename(columns={'visits': 'xgb'}, inplace=True)
    # Load baseline
    baseline = median_forecast(remove_outliers=True)
    # Save submission
    submission = pd.read_csv(os.path.join('..', 'input', 'key_2.csv'))
    submission['Date'] = submission.Page.apply(lambda a: a[-10:])
    submission['Page'] = submission.Page.apply(lambda a: a[:-11])
    submission['Date'] = submission['Date'].astype('datetime64[ns]')
    submission = submission.merge(results, how='left')
    submission = submission.merge(baseline, how='left')
    submission['Visits'] = submission.xgb.combine_first(submission.baseline)
    submission[['Id', 'Visits']].to_csv(os.path.join('..', 'output', 'submission_xgb.csv'), index=False)
