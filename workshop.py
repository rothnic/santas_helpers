__author__ = 'nickroth'

import pandas as pd
import numpy as np
import simpy
import arrow
from data.data_utils import load_toy_orders
from workers import Elf
from toys import Toy
from goal import T_REFERENCE
from event_logging import EventLogger, Event
from core import Population

WORK_START = 9
WORK_STOP = 19
MAX_ELVES = 900


class SantasWorkshop(Population):
    def __init__(self, env, toy_orders, logger):
        super(SantasWorkshop, self).__init__()
        self.env = env
        self.logger = logger
        self.toy_orders = toy_orders
        self.work_log = WorkLogger(len(self.toy_orders.index))
        self.elves = [Elf(workshop=self, logger=logger) for i in xrange(900)]
        self.elf_table = SantasWorkshop.init_elf_table(900)
        self.working_elves = []
        self.current_orders = simpy.Store(self.env)
        self.scheduler_proc = self.env.process(self.scheduler())
        self.toy_proc = self.env.process(self.produce_toys())

    def produce_toys(self):
        num_toys = len(self.toy_orders.index)
        idx = 0
        while idx <= num_toys:
            for idx, order in self.toy_orders.iterrows():
                if self.env.now < order.Rel_Arrival_time:
                    yield self.env.timeout(order.Rel_Arrival_time - self.env.now)
                self.current_orders.put(Toy(order_time=order.Rel_Arrival_time, build_duration=order.Duration))

    def toy_order(self, rel_time, duration):
        # yield self.env.timeout(rel_time)
        # self.logger.log(Event(self.datetime(), 'order received'))
        #
        self.current_orders.put(Toy(order_time=rel_time, build_duration=duration))

    def scheduler(self):
        while True:
            yield self.env.timeout(10)
            if self.is_worktime:
                this_toy = yield self.current_orders.get()
                this_elf = self.get_best_candidate()
                while len(self.current_orders.items) >= 0 and this_elf is not None:
                    self.start_work(this_elf.id)
                    this_elf.build(this_toy)
                    this_toy = yield self.current_orders.get()
                    this_elf = self.get_best_candidate()

    def update_elf_table(self):
        for elf in self.elves:
            self.elf_table.ix[elf.id, 'resting'] = elf.resting
            self.elf_table.ix[elf.id, 'resting_until'] = elf.resting_until

    def get_best_candidate(self):
        self.elf_table.sort(['productivity'], ascending=[0])
        candidates = self.elf_table[((self.elf_table.working == False) & (self.elf_table.resting == False))]
        if len(candidates.index) > 0:
            top_candidate = candidates.iloc[0]
            return self.elves[top_candidate.name]
        else:
            return None

    def complete_toy(self, toy):
        self.work_log.log(toy)

    def start_work(self, elf_id):
        self.elf_table.ix[elf_id, 'working'] = True

    def stop_work(self, elf_id):
        self.elf_table.ix[elf_id, 'working'] = False

    @property
    def is_worktime(self):
        return WORK_START <= self.datetime().hour < WORK_STOP

    def datetime(self):
        return arrow.get(self.now_timestamp)

    @property
    def now_timestamp(self):
        return T_REFERENCE.replace(minutes=int(self.env.now)).timestamp

    def rel_datetime(self, minutes):
        return T_REFERENCE.replace(minutes=int(minutes))

    def datetime_hour(self, the_datetime=None, hour=0):
        if the_datetime is None:
            the_datetime = self.datetime()
        return the_datetime.floor('day').replace(hour=hour)

    def rest_time(self, rest_hours):
        """
        Returns the number of minutes the Elf should rest, if he worked in unsanctioned hours.
        :param rest_hours:
        :return:
        """
        total_timeout = 0.0
        if rest_hours == 0.0:
            return 0.0

        today_start = self.datetime_hour(hour=WORK_START)
        tomorrow_start = today_start.replace(hours=24)
        today_end = self.datetime_hour(hour=WORK_STOP)
        rest_minutes = rest_hours * 60.0

        if self.is_worktime:
            today_timeout = max((today_end.timestamp - self.now_timestamp), rest_minutes)
            remain_timeout = (tomorrow_start.timestamp - today_end.timestamp) + (rest_minutes - today_timeout)
            return today_timeout + remain_timeout
        else:
            return ((tomorrow_start.timestamp - self.now_timestamp) / 60.0) + rest_minutes

    def get_hours_type(self, toy):
        start_time = self.rel_datetime(toy.build_start)
        # today_start = self.datetime_hour(the_datetime=start_time, hour=WORK_START)
        end_time = self.rel_datetime(toy.build_finish)
        today_end = self.datetime_hour(the_datetime=end_time, hour=WORK_STOP)

        min_total = toy.build_finish - toy.build_start

        min_non_sanctioned = (end_time.timestamp - today_end.timestamp) / 60.0  # seconds to minutes
        if min_non_sanctioned < 0.0:
            min_non_sanctioned = 0.0

        min_sanctioned = min_total - min_non_sanctioned

        return min_sanctioned / 60.0, min_non_sanctioned / 60.0

    @staticmethod
    def init_elf_table(count):
        productivity = np.ones((count, 1))
        working = np.zeros((count, 1), dtype=bool)
        working_until = np.zeros((count, 1))
        resting = np.zeros((count, 1), dtype=bool)
        resting_until = np.zeros((count, 1))
        data = np.hstack((productivity, working, working_until, resting, resting_until))
        df = pd.DataFrame(data=data, columns=['productivity', 'working', 'working_until',
                                              'resting', 'resting_until'])
        return df


class WorkLogger(object):
    def __init__(self, item_count):
        columns = ['ElfId', 'ToyId', 'StartTime', 'Duration']
        toy_id = np.zeros((item_count, 1))
        elf_id = np.zeros((item_count, 1))
        start_time = np.zeros((item_count, 1))
        duration = np.zeros((item_count, 1))
        data = np.hstack((toy_id, elf_id, start_time, duration))
        self.work_log = pd.DataFrame(data=data, columns=columns)

    def log(self, toy):
        actual_duration = toy.build_finish - toy.build_start
        self.work_log.ix[toy.id, 'ElfId'] = toy.elf_id
        self.work_log.ix[toy.id, 'ToyId'] = toy.id
        self.work_log.ix[toy.id, 'StartTime'] = toy.build_start
        self.work_log.ix[toy.id, 'Duration'] = actual_duration

    def save(self):
        self.work_log.to_csv('./work_log.csv')


if __name__ == '__main__':
    LOG_LEVEL = 'HIGH'
    LOG_RESOLUTION = 0.001

    sim = simpy.Environment()
    orders = load_toy_orders()

    ev = EventLogger(env=sim, level=LOG_LEVEL, resolution=LOG_RESOLUTION)
    sw = SantasWorkshop(env=sim, toy_orders=orders, logger=ev)
    sim.run()
    sw.work_log.save()