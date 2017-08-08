# Web Traffic Time Series Forecasting

Analysis and submissions code for the Kaggle competition. The idea is to forecast future traffic to Wikipedia pages. This competition focuses on the problem of forecasting the future values of multiple time series, as it has always been one of the most challenging problems in the field.

## Ideas

* Use Facebook's [prophet](https://github.com/facebookincubator/prophet) as a baseline
* Use median values as a baseline
* Remove outliers/smoothing
* Include additional features to the data such as Google Trends

## Useful links

* https://www.kaggle.com/c/web-traffic-time-series-forecasting
* https://github.com/facebookincubator/prophet
* Outlier detection and smoothing
    * https://www.quora.com/How-do-I-find-the-outliers-in-time-series-data
    * https://stats.stackexchange.com/questions/1142/simple-algorithm-for-online-outlier-detection-of-a-generic-time-series
    * https://ocefpaf.github.io/python4oceanographers/blog/2015/03/16/outlier_detection/
    * https://en.wikipedia.org/wiki/CUSUM
    * https://jalobe.com/blog/tsoutliers/
    * https://stats.stackexchange.com/questions/69874/how-to-correct-outliers-once-detected-for-time-series-data-forecasting
* Popular kernels
    * https://www.kaggle.com/headsortails/wiki-traffic-forecast-exploration-wtf-eda
    * https://www.kaggle.com/muonneutrino/wikipedia-traffic-data-exploration
    * https://www.kaggle.com/opanichev/simple-model
    * https://www.kaggle.com/cpmpml/smape-weirdness
    * https://www.kaggle.com/clustifier/weekend-weekdays

## Timeline

This competition has a training phase and a future forecasting phase. During the training phase, participants build models and predict on historical values. During the future phase, participants will forecast future traffic values.

* September 1st, 2017 - Deadline to accept competition rules.
* September 1st, 2017 - Team Merger deadline. This is the last day participants may join or merge teams.
* September 1st, 2017 - Final dataset is released.
* September 10th, 2017 - Final submission deadline.

Competition winners will be revealed after November 10, 2017.

All deadlines are at 11:59 PM UTC on the corresponding day unless otherwise noted. The competition organizers reserve the right to update the contest timeline if they deem it necessary.
