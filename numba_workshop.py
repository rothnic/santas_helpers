__author__ = 'rothnic'

import numpy as np
from numba import jit
from data.data_utils import load_toy_orders, START_MINUTE, to_minutes
import timeit


NUM_TOYS = 10000000
NUM_ELVES = 900
BATCH_CYCLE = 30  # minutes
MINUTES_PER_DAY = 1440

PROD_PENALTY = 0.9
PROD_REWARD = 1.02
MIN_PROD = 0.25
MAX_PROD = 4.0


def run_workshop():
    the_days = days('2014-01-01', 75)
    toy_orders = load_toy_orders()

    toy_id = toy_orders.index
    toy_duration = toy_orders['Duration'].values
    toy_completion = np.zeros(NUM_TOYS)
    toy_arrival_min = to_minutes(toy_orders['Relative_minutes'].values)
    toy_processed = np.zeros(NUM_TOYS, dtype=bool)
    batch_idx = np.zeros(NUM_TOYS, dtype=bool)

    elf_working_until = np.zeros(NUM_ELVES)
    elf_resting_left = np.zeros(NUM_ELVES)
    elf_productivity = np.ones(NUM_ELVES)
    eligible = np.zeros(NUM_ELVES, dtype=bool)

    i = 0
    MAX_DAYS = np.size(the_days)
    #MAX_DAYS = 5

    # loop over each day
    while i < MAX_DAYS and not np.all(toy_processed):

        the_day = the_days[i]
        print(the_day)
        print('Toys Left: ' + str(np.sum(np.logical_not(toy_processed))))

        # loop over each hour
        for j in xrange(0, 24):
            the_hour = np.timedelta64(j, 'h')
            working = work_hours(the_hour)
            the_minute = to_minutes(the_day + the_hour) - START_MINUTE

            # if available elves and working and available toys
            # if np.all(elf_resting_left > the_minute + 60):
            #
            #     # handle long resting periods to avoid the minute loop
            #     update_resting(elf_resting_left, amount=60.0)

            if working:

                # update batch only when working
                batch_idx = get_batch(the_minute, toy_arrival_min, toy_processed, batch_idx)

                # loop over each minute so that we can continue to assign elves to short duration toys
                for k in xrange(0, 60):
                    the_minute += k

                    update_resting(elf_resting_left, elf_working_until, the_minute)

                    if np.any(batch_idx):

                        # see if we have elves that can be used now
                        if np.any(np.logical_and(elf_working_until < the_minute, elf_resting_left <= 0.0)):

                                # get a pairing between valid elfs and toys, where we have a reference to their indices
                            elf_indices, toy_indices = allocate_elfs(the_minute, batch_idx, toy_duration, elf_working_until,
                                                                     elf_resting_left, elf_productivity, eligible)

                            # set the times when these toys are complete
                            build_toys(the_minute, elf_indices, elf_productivity, elf_working_until, toy_indices,
                                       toy_duration, toy_completion, toy_processed)

                            # figure out how long we must rest, and get the minutes that were worked in sanction/unsanctioned times
                            print("" + str(the_day) + " " + str(the_hour) + " " + str(the_minute))
                            sanctioned, unsanctioned = set_rest_minutes(the_hour, the_minute, elf_indices,
                                                                        elf_working_until, elf_resting_left)

                            # update the elf productivity based on when the toys were built
                            update_productivity(elf_indices, elf_productivity, sanctioned, unsanctioned)

                            # set toys from this batch as processed
                            batch_idx[toy_indices] = False

        i += 1

    save_results()


def save_results():
    pass


def days(start_year, num_years):
    start_year = np.datetime64(start_year)
    delta = np.timedelta64(1, 'D')
    return np.array([start_year + delta * i for i in xrange(365 * num_years)])


def work_hours(the_hour):
    return 9 <= the_hour < 19


def update_productivity(elf_indices, elf_productivity, sanctioned, unsanctioned):
    elf_productivity[elf_indices] = (elf_productivity[elf_indices] * (PROD_REWARD ** (sanctioned / 60.0)) *
                                     (PROD_PENALTY ** (unsanctioned / 60.0)))
    elf_productivity[elf_productivity < 0.25] = 0.25
    elf_productivity[elf_productivity > 4.0] = 4.0


# @jit
def set_rest_minutes(the_hour, the_minute, elf_indices, working_until, resting_minutes):
    """
    Sets the time when the elf can start working again based on the duration they must work.
    :param the_hour:
    :param the_minute:
    :param elf_indices:
    :param resting_minutes:
    :return:
    """

    resting_minutes[elf_indices] = 0.0
    this_resting_minutes = resting_minutes[elf_indices]
    sanctioned = np.zeros_like(this_resting_minutes)
    unsanctioned = np.zeros_like(this_resting_minutes)
    working_duration = working_until[elf_indices] - the_minute

    # handle today's remaining time
    this_minute = the_minute % 60
    minutes_left = ((19 - the_hour.astype('int64')) * 60) - this_minute
    # working_overtime = working_until > minutes_left


    this_hour = the_hour

    # working left within hours
    finish_within_period = (working_duration - minutes_left.astype('int64')) < 0
    within_hours = np.copy(working_duration)
    within_hours[np.logical_not(finish_within_period)] = minutes_left
    # Todo: detect where we finish within the hour batching period

    # add to sanctioned working hours
    sanctioned += within_hours

    # remove the hours we worked
    working_duration -= within_hours

    # get the elves that start working overtime
    still_working = working_duration > 0
    if np.any(still_working):

        periods = ['afternoon', 'morning', 'working']
        durations = [300, 540, 600]

        while np.any(still_working):
            for i in range(len(periods)):
                dur = durations[i]

                # get the number we worked by either taking the time left, or difference in time left and length of the
                # current working period that were are addressing
                finish_within_period = (working_duration - dur) < 0
                worked_hours = np.copy(working_duration)
                worked_hours[np.logical_not(finish_within_period)] = dur

                # remove the hours worked from the time left we need to work on the toys
                working_duration[still_working] -= worked_hours[still_working]

                # first two periods are unsanctioned hours, last is sanctioned
                if i < 2:
                    unsanctioned[still_working] += worked_hours[still_working]
                else:
                    sanctioned[still_working] += worked_hours[still_working]

                # see if we are still working
                still_working = working_duration > 0

    return sanctioned, unsanctioned


def build_toys(the_minute, elf_indices, elf_productivity, elf_working_until, toy_indices, toy_duration, toy_completion, toy_processed):
    """
    Sets the time when the toy completes construction, based on assigned elf's productivity and toy's build duration
    :return:
    """
    ending_minute = the_minute + (toy_duration[toy_indices] / elf_productivity[elf_indices])

    toy_completion[toy_indices] = ending_minute
    toy_processed[toy_indices] = True

    elf_working_until[elf_indices] = ending_minute
    return toy_completion[toy_indices]


def update_work(work_left, duration_left):
    work_done_idx = work_left > 0
    work_left[work_done_idx] -= 1
    duration_left_idx = duration_left > 0
    duration_left[duration_left_idx] -= 1


def update_resting(elf_resting_left, elf_working_until, the_minute, amount=1.0):
    elf_resting_left[elf_working_until <= the_minute] -= amount
    elf_resting_left[elf_resting_left < 0] = 0.0


@jit
def get_batch_fast(the_time, arrival_minutes, toy_processed, batch_idx):
    """
    Batches the indices of the unprocessed toys as they arrive.
    :param the_time:
    :param arrival_minutes:
    :param toy_processed:
    :param batch_idx:
    :return:
    """
    return np.logical_and((arrival_minutes <= the_time), (toy_processed == False), batch_idx)


def get_batch(the_time, arrival_minutes, toy_processed, batch_idx):
    """
    Batches the indices of the unprocessed toys as they arrive.
    :param the_time:
    :param arrival_minutes:
    :param toy_processed:
    :param batch_idx:
    :return:
    """
    return get_batch_fast(the_time, arrival_minutes, toy_processed, batch_idx)


@jit
def get_batch_loop(the_time, arrival_minutes, toy_processed, batch_idx):
    """
    Get the current batch using a numba-compiled loop, vice numpy array operators
    :param the_time:
    :param arrival_minutes:
    :param toy_processed:
    :param batch_idx:
    :return:
    """
    for i in range(batch_idx.size):
        batch_idx[i] = arrival_minutes[i] <= the_time & toy_processed[i] == False
    return batch_idx


def allocate_elfs(the_minute, batch_idx, duration, elf_working_until, elf_resting_left, productivity, eligible):
    eligible = np.logical_and((elf_working_until <= the_minute), (elf_resting_left <= 0.0), eligible)
    # elig_elf_indices = np.where(eligible)[0]
    toy_indices = np.where(batch_idx)[0]

    # get productivity of eligible elves
    elig_prod = productivity[eligible]

    # only get number of toys as we have eligible elves
    toy_indices = toy_indices[:np.sum(eligible)]
    num_toys = toy_indices.size

    elig_duration = duration[toy_indices]

    # sort descending
    prod_sort = elig_prod.argsort()[::-1]
    elig_elf_indices = prod_sort[:min(num_toys, prod_sort.size)]

    return elig_elf_indices, toy_indices


run_workshop_jit = jit(run_workshop)

if __name__ == "__main__":
    run_workshop()