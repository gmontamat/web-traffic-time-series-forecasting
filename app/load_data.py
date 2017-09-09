#!/usr/bin/env python
"""
Functions to load input files into memory or a database system
"""

import datetime
import os
import pandas as pd

from dao import SimpleDao

TRAIN_FILE = 'train_2.csv'
# Get list of days
min_date = datetime.datetime.strptime('2017-01-01', '%Y-%m-%d')
max_date = datetime.datetime.strptime('2017-08-31', '%Y-%m-%d')
# Will miss 2017-08-31 :(
dates = [(min_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in xrange((max_date - min_date).days)]
COLUMNS = ["Page"] + dates


def load_data(file_path):
    return pd.read_csv(file_path, delimiter=',', quotechar='"', usecols=COLUMNS)


def load_train():
    app_path = os.path.dirname(os.path.realpath(__file__))
    return load_data(os.path.join(app_path, '..', 'input', TRAIN_FILE))


def load_flat_train():
    flat = pd.melt(load_train(), id_vars=['Page'], var_name='Date', value_name='Visits')
    flat['Date'] = flat['Date'].astype('datetime64[ns]')
    return flat


if __name__ == '__main__':
    train_flat = load_flat_train()
    dao = SimpleDao('localhost', '5432', 'postgres', 'postgres', 'kaggle-wttsf')
    dao.upload_from_dataframe(train_flat, 'train2_flat')
