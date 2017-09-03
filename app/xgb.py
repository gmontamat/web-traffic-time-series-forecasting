#!/usr/bin/env python
"""
Train an Xgboost model
"""

import os
import cPickle
import xgboost as xgb


class XgboostRegressor(object):

    def __init__(self, params=None, model_path=None, name='xgboost'):
        self.name = name
        if model_path:
            self.params, self.model = self.load_model(model_path, name)
            self.ready = True
        else:
            self.model = None
            if params:
                self.params = params
            else:
                # Default parameters
                self.params = {
                    'booster': 'gbtree',
                    'objective': 'reg:linear',
                    'eval_metric': 'rmse',
                    'eta': 0.1,
                    'max_depth': 4,
                    'min_child_weight': 1,
                    'subsample': 0.7,
                    'colsample_bytree': 0.7
                }
            self.ready = False

    def train_model(self, x_train, y_train):
        d_train = xgb.DMatrix(x_train, label=y_train)
        cv_result = xgb.cv(
            self.params, d_train, num_boost_round=2000, early_stopping_rounds=50, verbose_eval=50, show_stdv=False
        )
        num_boost_rounds = len(cv_result)
        self.model = xgb.train(self.params, d_train, num_boost_round=num_boost_rounds)
        self.ready = True

    @staticmethod
    def load_model(model_path, name):
        # Load model
        model = xgb.Booster()
        model.load_model(os.path.join(model_path, '{}.bin'.format(name)))
        # Load additional parameters
        with open(os.path.join(model_path, '{}.pkl'.format(name)), 'rb') as fin:
            params = cPickle.load(fin)
        return params, model

    def save_model(self, model_path):
        if not self.ready:
            raise ValueError("Model not fitted")
        self.model.save_model(os.path.join(model_path, '{}.bin'.format(self.name)))
        # Save model parameters
        with open(os.path.join(model_path, '{}.pkl'.format(self.name)), 'wb') as fout:
            cPickle.dump(self.params, fout)

    def predict(self, x):
        if not self.ready:
            raise AttributeError("Model not fitted")
        return self.model.predict(xgb.DMatrix(x))
