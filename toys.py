__author__ = 'nickroth'

from core import Population


class Toy(Population):
    def __init__(self, order_time, build_duration):
        super(Toy, self).__init__()
        self.toy_id = self.id + 1
        self.order_time = order_time
        self.build_duration = build_duration
        self.build_start = None
        self.build_finish = None
        self.elf_id = None

    def __str__(self):
        return super(Toy, self).__str__() + ' (build duration: ' + str(self.build_duration) + ')'