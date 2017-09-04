#!/usr/bin/env python
"""
Functions to evaluate forecast accuracy
SMAPE: https://en.wikipedia.org/wiki/Symmetric_mean_absolute_percentage_error
"""

import numpy as np


def smape(y_true, y_pred):
    denominator = (np.abs(y_true) + np.abs(y_pred))
    diff = np.abs(y_true - y_pred) / denominator
    diff[denominator == 0] = 0.0
    return 200 * np.mean(diff)
