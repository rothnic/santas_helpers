__author__ = 'nickroth'

import pandas as pd
import numpy as np
import arrow
import os
from goal import T_REFERENCE

this_dir = os.path.dirname(os.path.realpath(__file__))
data_file = os.path.join(this_dir, 'data.h5')


def convert_data(csv_path):
    store = pd.HDFStore(data_file)
    print('Loading CSV File')
    df = pd.DataFrame.from_csv(csv_path)
    print('CSV File Loaded, Converting Dates/Times')
    df['Arrival_time'] = map(convert_time, df['Arrival_time'])
    df['Rel_Arrival_time'] = (df['Arrival_time'] - T_REFERENCE.timestamp)/60.0
    print('Conversion Complete')
    store['toy_orders'] = df

def convert_time(this_time):
    parsed_date = arrow.get(this_time, 'YYYY M D H m')
    return parsed_date.timestamp

def load_toy_orders():
    store = pd.HDFStore(data_file)
    return store['toy_orders']


if __name__ == '__main__':
    convert_data('toys_rev2.csv')