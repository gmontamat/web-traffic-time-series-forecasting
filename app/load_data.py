#!/usr/bin/env python

"""
Functions to load input files into memory or a database system
"""

import os
import pandas as pd

from dao import SimpleDao


def load_data(file_path):
    return pd.read_csv(file_path, delimiter=',', quotechar='"')


def load_train():
    app_path = os.path.dirname(os.path.realpath(__file__))
    return load_data(os.path.join(app_path, '..', 'input', 'train_1.csv'))


def load_flat_train():
    flat = pd.melt(load_train(), id_vars=['Page'], var_name='Date', value_name='Visits')
    flat['Date'] = flat['Date'].astype('datetime64[ns]')
    return flat


if __name__ == '__main__':
    train_flat = load_flat_train()
    dao = SimpleDao('localhost', '5432', 'postgres', 'postgres', 'kaggle-wttsf')
    dao.upload_from_dataframe(train_flat, 'train_flat')
