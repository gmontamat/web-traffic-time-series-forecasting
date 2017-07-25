#!/usr/bin/env python

"""
fbprophet's forecast
"""

import os
import pandas as pd
import progressbar

from fbprophet import Prophet
from load_data import load_train


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
        os.close(self.save_fds[0])
        os.close(self.save_fds[1])


class ProphetForecaster(object):

    def __init__(self, yearly_seasonality=True):
        self.train = load_train()
        self.yearly_seasonality = yearly_seasonality

    def forecast(self):
        forecasts = pd.DataFrame(columns=['Page', 'Visits'])
        ctr = 1
        bar = progressbar.ProgressBar()
        for i in bar(xrange(len(self.train))):
            try:
                series_name = self.train.loc[i][0]
                series_values = pd.DataFrame(pd.to_numeric(self.train.loc[i][1:]))
                series_values.reset_index(level=0, inplace=True)
                series_values.columns = ['ds', 'y']
                with suppress_stdout_stderr():
                    model = Prophet(yearly_seasonality=self.yearly_seasonality).fit(series_values)
                future = model.make_future_dataframe(periods=60, include_history=False)
                forecast = model.predict(future)
                forecast['Page'] = series_name + '_' + forecast['ds'].astype(str)
                forecasts = forecasts.append(forecast[['Page', 'yhat']].rename(columns={'yhat': 'Visits'}))
            except Exception as e:
                with open(os.path.join('..', 'output', 'prophet_errors.txt'), 'a') as fout:
                    fout.write('{},{}\n'.format(i, str(e)))
            # Free memory?
            if len(forecasts) > 1000000:
                output_file = os.path.join('..', 'output', 'prophet_part{}.csv'.format(str(ctr).zfill(3)))
                forecasts.to_csv(output_file, index=False)
                ctr += 1
                forecasts = pd.DataFrame(columns=['Page', 'Visits'])
        output_file = os.path.join('..', 'output', 'prophet_part{}.csv'.format(str(ctr).zfill(3)))
        forecasts.to_csv(output_file, index=False)

    def forecast_errors(self):
        # Get series that failed
        errors = pd.read_csv(os.path.join('..', 'output', 'prophet_errors.txt'), header=None)
        indexes = errors[0].values
        # Build a response of zeros
        series_name = self.train.loc[0][0]
        series_values = pd.DataFrame(pd.to_numeric(self.train.loc[0][1:]))
        series_values.reset_index(level=0, inplace=True)
        series_values.columns = ['ds', 'y']
        with suppress_stdout_stderr():
            model = Prophet().fit(series_values)
        future = model.make_future_dataframe(periods=60, include_history=False)
        forecast_zeros = model.predict(future)
        forecast_zeros['Page'] = series_name + '_' + forecast_zeros['ds'].astype(str)
        forecast_zeros['yhat'] = 0.0
        # Forecast previous errors
        forecasts = pd.DataFrame(columns=['Page', 'Visits'])
        bar = progressbar.ProgressBar()
        for i in bar(indexes):
            series_name = self.train.loc[i][0]
            series_values = pd.DataFrame(pd.to_numeric(self.train.loc[i][1:]))
            series_values.reset_index(level=0, inplace=True)
            series_values.columns = ['ds', 'y']
            with suppress_stdout_stderr():
                try:
                    if sum(series_values.fillna(0.0)['y']) < 1.0:
                        model = None
                    else:
                        model = Prophet().fit(series_values)
                except Exception:
                    try:
                        model = Prophet(n_changepoints=20).fit(series_values)
                    except Exception:
                        model = None
            if model is not None:
                future = model.make_future_dataframe(periods=60, include_history=False)
                forecast = model.predict(future)
                forecast['Page'] = series_name + '_' + forecast['ds'].astype(str)
            else:
                forecast = forecast_zeros.copy()
                forecast['Page'] = series_name + '_' + forecast['ds'].astype(str)
            forecasts = forecasts.append(forecast[['Page', 'yhat']].rename(columns={'yhat': 'Visits'}))
        output_file = os.path.join('..', 'output', 'prophet_partXXX.csv')
        forecasts.to_csv(output_file, index=False)

    @staticmethod
    def concatenate_parts():
        parts = []
        for root, dirs, files in os.walk(os.path.join('..', 'output')):
            for file in files:
                if 'prophet_part' in file:
                    parts.append(pd.read_csv(os.path.join(root, file)))
        forecasts = pd.concat(parts)
        forecasts = forecasts.clip(lower=0.0)   # Replace negative forecasts with 0's
        forecasts.to_csv(os.path.join('..', 'output', 'prophet.csv'), index=False)

    @staticmethod
    def generate_submission(clip_output=True, round_output=True):
        submission = pd.merge(
            pd.read_csv(os.path.join('..', 'input', 'key_1.csv')),
            pd.read_csv(os.path.join('..', 'output', 'prophet_nys.csv')),
            how='inner', on='Page'
        )
        if clip_output:
            submission = submission.clip(lower=0.0)   # Replace negative forecasts with 0's
        if round_output:
            submission['Visits'] = submission['Visits'].apply(lambda x: round(x, 0))
        submission.drop('Page', axis=1).to_csv(os.path.join('..', 'output', 'submission.csv'), index=False)


if __name__ == '__main__':
    ProphetForecaster.concatenate_parts()
    ProphetForecaster.generate_submission()
