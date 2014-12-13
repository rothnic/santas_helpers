__author__ = 'nickroth'

import numpy as np
import arrow

T_REFERENCE = arrow.get(2014, 1, 1)
T_START = arrow.get(2014, 1, 1, 9)
TOY_GOAL = np.int(10000000)

def objective(t_final, num_elves):
    return t_final * np.log(1 + num_elves)

if __name__ == '__main__':

    pass