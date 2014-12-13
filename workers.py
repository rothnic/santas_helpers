__author__ = 'nickroth'

from core import Population
from event_logging import Event

PROD_PENALTY = 0.9
PROD_REWARD = 1.02
MIN_PROD = 0.25
MAX_PROD = 4.0


class Elf(Population):

    def __init__(self, workshop, logger):
        super(Elf, self).__init__()
        self.productivity = 1.0
        self.workshop = workshop
        self.logger = logger
        self.env = self.workshop.env
        self.resting = False
        self.resting_until = 0

    def build(self, toy):
        self.logger.log(Event(self.workshop.datetime(), self, 'started working', toy))
        self.workshop.elf_table.ix[self.id, 'working'] = True
        toy.elf_id = self.id
        toy.build_start = self.env.now
        self.workshop.elf_table.ix[self.id, 'working_until'] = self.workshop.datetime().replace(
            minutes=(toy.build_duration / self.productivity))
        self.env.process(self.build_toy(toy))

    def update_productivity(self, toy):
        """
        Updates Elf productivity based on how the work times fall within the span of valid work hours. It then makes
        sure the updated productivity doesn't fall outside of configured max and minimum values.
        :param toy:
        :return:
        """
        h_sanctioned, h_unsanctioned = self.workshop.get_hours_type(toy)
        temp_productivity = self.productivity * (PROD_REWARD ** h_sanctioned) * (PROD_PENALTY ** h_unsanctioned)
        self.productivity = self.bound_productivity(temp_productivity)
        self.workshop.elf_table.ix[self.id, 'productivity'] = self.productivity

        if h_unsanctioned > 0.0:
            self.env.process(self.rest(h_unsanctioned))

    def rest(self, h_unsanctioned):
        rest_minutes = self.workshop.rest_time(h_unsanctioned)
        self.workshop.elf_table.ix[self.id, 'resting'] = True
        rest_period = self.workshop.datetime().replace(minutes=rest_minutes)
        self.workshop.elf_table.ix[self.id, 'resting_until'] = rest_period
        self.logger.log(Event(self.workshop.datetime(), self, 'resting until', rest_period))
        yield self.env.timeout(rest_minutes)
        self.workshop.elf_table.ix[self.id, 'resting'] = False
        self.workshop.elf_table.ix[self.id, 'resting_until'] = 0

    def build_toy(self, toy):

        yield self.env.timeout(toy.build_duration / self.productivity)
        self.workshop.elf_table.ix[self.id, 'working_until'] = 0
        #self.workshop.elf_table.ix[self.id, 'working'] = False
        self.workshop.stop_work(self.id)
        self.logger.log(Event(self.workshop.datetime(), self, 'stopped working', toy))
        toy.build_finish = self.env.now
        self.workshop.complete_toy(toy)
        self.update_productivity(toy)

    @staticmethod
    def bound_productivity(this_productivity):
        # cap productivity by min and maximum values
        if this_productivity < MIN_PROD:
            return MIN_PROD
        elif this_productivity > MAX_PROD:
            return MAX_PROD
        else:
            return this_productivity

    def __str__(self):
        return super(Elf, self).__str__() + ' (Productivity: ' + str(self.productivity) + ')'