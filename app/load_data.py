#!/usr/bin/env python

"""
Functions to load input files into memory
"""

import os
import pandas as pd


def load_data(file_path):
    return pd.read_csv(file_path, delimiter=',', quotechar='"')


def load_train():
    app_path = os.path.dirname(os.path.realpath(__file__))
    return load_data(os.path.join(app_path, '..', 'input', 'train_1.csv'))


if __name__ == '__main__':
    train = load_train()
