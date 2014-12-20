__author__ = 'rothnic'

import numpy as np
from numba import jit
from data.data_utils import load_toy_orders
import timeit

# ToDo: precalculate key time periods ahead of time (sanctioned time)

NUM_TOYS = 10000000
NUM_ELVES = 900
BATCH_CYCLE = 30  # minutes

def run_workshop():

    the_days = days('2014-01-01', 75)
    toy_orders = load_toy_orders()

    toy_id = np.array((NUM_TOYS, 1))
    toy_duration = np.array((NUM_TOYS, 1))
    toy_duration_left = np.ones((NUM_TOYS, 1)) * 100
    toy_arrival_min = np.array((NUM_TOYS, 1))
    toy_processed = np.zeros((NUM_TOYS, 1), dtype=bool)
    toy_finish = np.zeros((NUM_TOYS, 1))

    elf_id = np.array((NUM_ELVES, 1))
    elf_working = np.array((NUM_ELVES, 1))
    elf_working_until = np.array((NUM_ELVES, 1))
    elf_resting = np.array((NUM_ELVES, 1))
    elf_resting_complete = np.array((NUM_ELVES, 1))
    elf_productivity = np.ones((NUM_ELVES, 1))

    #while np.any(toy_duration_left > 0):
    for i in xrange(np.size(the_days)):

        the_day = the_days[i]

        # Get all toys that haven't been processed and have arrived
        batch_idx = get_batch(the_day, toy_arrival_min, toy_processed)

        if batch_idx is not None:
            allocate_elfs(toy_id, toy_duration, elf_id, elf_working, elf_resting, elf_productivity, elf_productivity)

        if work_hours():
            update_resting()

        update_work(toy_duration_left)


def days(start_year, num_years):
    start_year = np.datetime64(start_year)
    delta = np.timedelta64(1, 'D')
    return np.array([start_year + delta*i for i in xrange(365*num_years)])


def work_hours():
    return True


def update_work(work_left, duration_left):
    work_done_idx = work_left > 0
    work_left[work_done_idx] -= 1
    duration_left_idx = duration_left > 0
    duration_left[duration_left_idx] -= 1


def update_resting():
    pass


def get_batch(the_minute, arrival_minutes, toy_processed):
    batch_idx = (arrival_minutes <= the_minute) and (toy_processed == False)
    if not np.any(batch_idx):
        batch_idx = None
    return batch_idx


def allocate_elfs(toy_ids, duration, elf_ids, working, working_until, resting, productivity):
    eligible = (working == False) and (resting == False)
    num_toys = len(toy_ids)

    elig_prod = productivity[eligible]
    elig_id = elf_ids[eligible]
    elig_duration = duration[eligible]

    prod_sort = elig_prod.argsort()
    prod_sort = prod_sort[:num_toys]

    elig_id = elig_id[prod_sort]
    elig_prod = elig_prod[prod_sort]






run_workshop_jit = jit(run_workshop)

if __name__ == "__main__":

    run_workshop()