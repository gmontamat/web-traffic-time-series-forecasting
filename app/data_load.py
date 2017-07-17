#!/usr/bin/env python

"""
Functions to load CSV files into memory
"""

import pandas as pd


def load(file_path):
    return pd.read_csv(file_path, delimiter=',', quotechar='"')


if __name__ == '__main__':
    train = load('../data/train_1.csv')
