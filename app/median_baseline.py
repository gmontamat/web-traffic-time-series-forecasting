#!/usr/bin/env python

"""
Use median as prediction
Source: https://www.kaggle.com/clustifier/weekend-weekdays/code
"""

import os
import pandas as pd

from load_data import load_train


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


if __name__ == '__main__':
    median_week()
