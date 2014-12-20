__author__ = 'nickroth'

import pandas as pd
import numpy as np
import arrow
import os
from goal import T_REFERENCE

this_dir = os.path.dirname(os.path.realpath(__file__))
data_file = os.path.join(this_dir, 'data.h5')

def load_toy_csv(csv_path):
    df = pd.DataFrame.from_csv(csv_path)
    return df

def convert_data(csv_path):
    store = pd.HDFStore(data_file)
    print('Loading CSV File')
    df = load_toy_csv(csv_path)
    print('CSV File Loaded, Converting Dates/Times')

    # parse string
    df['date_str'] = df['Arrival_time'].map(convert_time)

    # split up time components
    df['Year'], df['Month'], df['Day'], df['Hour'], df['Minute'] = zip(*df['Arrival_time'].map(split_time))

    # parse time into number of minutes
    df['minutes'] = pd.to_datetime(df['date_str'], format='%Y-%m-%d %H:%M:%S', unit='m').astype('int64') / \
                    np.timedelta64(1, 'm').astype('timedelta64[ns]').astype('int64')

    # pre-compute work hours
    df['Work_Hours'] = np.logical_and((df['Hour'].values < 19), (df['Hour'].values >= 9))

    df['Relative_minutes'] = df['minutes'] - df.ix[1, 'minutes']

    #df['Rel_Arrival_time'] = (df['Arrival_time'] - T_REFERENCE.timestamp)/60.0
    print('Conversion Complete')
    store['toy_orders'] = df


def calc_rel_arrival(Year, Month, Day, Hour, Minute):
    years = np.asarray([(str(yr) + '0101T000Z') for yr in Year.values.tolist()], dtype='datetime64[Y]')
    months = Month.values.astype('timedelta64[M]')
    days = Day.values.astype('timedelta64[D]')
    hours = Hour.values.astype('timedelta64[h]')
    minutes = Minute.values.astype('timedelta64[m]')
    return (years + (months - np.timedelta64(1, 'M')) + (days - np.timedelta64(1, 'D')) + hours + minutes) - \
           np.datetime64('2014-1-1')


def convert_time(this_time):
    """
    Returns the time component by index position. 0=Year, 1=Month, 2=Day, 3=Hour, 4=Minute
    :param this_row:
    :return:
    """
    times = this_time.split(' ')
    time_components = ['Year', 'Month', 'Day', 'Hour', 'Minute']
    if len(times[3]) == 1:
        times[3] = '0' + times[3]

    if len(times[4]) == 1:
        times[4] = '0' + times[4]

    time_str = '-'.join(times[0:3]) + ' ' + times[3] + ':' + times[4] + ':00'
    return time_str


def split_time(this_time):
    """
    Returns the time component by index position. 0=Year, 1=Month, 2=Day, 3=Hour, 4=Minute
    :param this_row:
    :return:
    """
    times = this_time.split(' ')
    time_components = ['Year', 'Month', 'Day', 'Hour', 'Minute']
    return (int(times[idx]) for idx, time_component in enumerate(time_components))

def load_toy_orders():
    store = pd.HDFStore(data_file)
    return store['toy_orders']


if __name__ == '__main__':
    convert_data('toys_rev2.csv')