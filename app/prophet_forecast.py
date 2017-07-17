#!/usr/bin/env python

"""
fbprophet's forecast
"""

import os
import pandas as pd
import progressbar

from fbprophet import Prophet
from data_load import load


# pd.options.mode.chained_assignment = None


class suppress_stdout_stderr(object):
    """
    A context manager for doing a "deep suppression" of stdout and stderr in
    Python, i.e. will suppress all print, even if the print originates in a
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).

    """
    def __init__(self):
        # Open a pair of null files
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = (os.dup(1), os.dup(2))

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0], 1)
        os.dup2(self.null_fds[1], 2)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0], 1)
        os.dup2(self.save_fds[1], 2)
        # Close the null files
        os.close(self.null_fds[0])
        os.close(self.null_fds[1])
        # Close
        os.close(self.save_fds[0])
        os.close(self.save_fds[1])


def prophet_forecast():
    forecasts = pd.DataFrame(columns=['Page', 'Visits'])
    ctr = 1
    train = load('../data/train_1.csv')
    bar = progressbar.ProgressBar()
    for i in bar(xrange(len(train))):
        try:
            series_name = train.loc[i][0]
            series_values = pd.DataFrame(pd.to_numeric(train.loc[i][1:]))
            series_values.reset_index(level=0, inplace=True)
            series_values.columns = ['ds', 'y']
            with suppress_stdout_stderr():
                model = Prophet().fit(series_values)
            future = model.make_future_dataframe(periods=60, include_history=False)
            forecast = model.predict(future)
            forecast['Page'] = series_name + '_' + forecast['ds'].astype(str)
            forecasts = forecasts.append(forecast[['Page', 'yhat']].rename(columns={'yhat': 'Visits'}))
        except Exception as e:
            with open('errors.txt', 'a') as fout:
                fout.write('{},{}\n'.format(i, str(e)))
        # Free memory?
        if len(forecasts) > 1000000:
            forecasts.to_csv('../data/prophet_part{}.csv'.format(str(ctr).zfill(3)), index=False)
            ctr += 1
            forecasts = pd.DataFrame(columns=['Page', 'Visits'])
    forecasts.to_csv('../data/prophet_part{}.csv'.format(str(ctr).zfill(3)), index=False)


if __name__ == '__main__':
    prophet_forecast()
