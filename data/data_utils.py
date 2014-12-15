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
    df['Year'], df['Month'], df['Day'], df['Hour'], df['Minute'] = zip(*df['Arrival_time'].map(convert_time))

    # pre-compute work hours
    df['Work_Hours'] = np.logical_and((df['Hour'].values < 19), (df['Hour'].values >= 9))

    df['Relative_minutes'] = calc_rel_arrival(df['Year'], df['Month'], df['Day'], df['Hour'], df['Minute'])

    #df['Rel_Arrival_time'] = (df['Arrival_time'] - T_REFERENCE.timestamp)/60.0
    print('Conversion Complete')
    store['toy_orders'] = df


def calc_rel_arrival(Year, Month, Day, Hour, Minute):
    years = np.datetime64(Year.values)
    months = np.timedelta64(Month.values, 'M')
    days = np.timedelta64(Day.values, 'D')
    hours = np.timedelta64(Hour.values, 'h')
    minutes = np.timedelta64(Minute.values, 'm')
    return (years + months + days + hours + minutes) / np.timedelta64(1, 'm')


def convert_time(this_time):
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