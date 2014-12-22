__author__ = 'nickroth'

import pandas as pd
import numpy as np
import os


this_dir = os.path.dirname(os.path.realpath(__file__))
data_file = os.path.join(this_dir, 'data.h5')


def load_toy_csv(csv_path):
    """
    Returns a dataframe for the csv.
    :param csv_path: string of the path to the csv
    :return: dataframe
    """
    df = pd.DataFrame.from_csv(csv_path)
    return df


def convert_data(csv_path):
    """
    Convert the given toy order csv file to a dataframe and store into HDF5 file for faster loading.
    :param csv_path: string path for csv file
    :return:
    """

    # open the HDF5 file
    store = pd.HDFStore(data_file)

    # open the csv file
    print('Loading CSV File')
    df = load_toy_csv(csv_path)
    print('CSV File Loaded, Converting Dates/Times')

    # parse string
    df['date_str'] = df['Arrival_time'].map(convert_time)

    # split up time components
    df['Year'], df['Month'], df['Day'], df['Hour'], df['Minute'] = zip(*df['Arrival_time'].map(split_time))

    # parse time into number of minutes
    df['minutes'] = to_minutes(pd.to_datetime(df['date_str'], format='%Y-%m-%d %H:%M:%S', unit='m'))

    # pre-compute work hours
    df['Work_Hours'] = np.logical_and((df['Hour'].values < 19), (df['Hour'].values >= 9))

    # calculate minutes relative to start of 2014
    df['Relative_minutes'] = df['minutes'] - df.ix[1, 'minutes']

    print('Conversion Complete')

    # save the dataframe into hdf5 store
    store['toy_orders'] = df


def to_minutes(the_datetimes):
    return the_datetimes.astype('datetime64[m]').astype('int64')


def convert_time(this_time):
    """
    Returns the time component by index position. 0=Year, 1=Month, 2=Day, 3=Hour, 4=Minute
    :param this_row:
    :return:
    """

    # split the given time by spaces
    times = this_time.split(' ')

    # make sure the time has two digits
    if len(times[3]) == 1:
        times[3] = '0' + times[3]

    if len(times[4]) == 1:
        times[4] = '0' + times[4]

    # build the valid time representation in a string format
    time_str = '-'.join(times[0:3]) + ' ' + times[3] + ':' + times[4] + ':00'
    return time_str


def split_time(this_time):
    """
    Returns the time component by index position. 0=Year, 1=Month, 2=Day, 3=Hour, 4=Minute
    :param this_row:
    :return:
    """

    # split time by empty space
    times = this_time.split(' ')

    # a list of times that are represented by the split times
    time_components = ['Year', 'Month', 'Day', 'Hour', 'Minute']

    # iterate over the list to return an integer for each time component within a tuple
    return (int(times[idx]) for idx, time_component in enumerate(time_components))


def load_toy_orders():
    """
    Returns the stored dataframe from the HDF5 storage.
    :return: preprocessed toy order dataframe
    """

    # open the hdf5
    store = pd.HDFStore(data_file)

    # return the toy orders dataframe, which is stored within it
    return store['toy_orders']


START_MINUTE = to_minutes(np.datetime64('2014-01-01'))


if __name__ == '__main__':
    convert_data('toys_rev2.csv')